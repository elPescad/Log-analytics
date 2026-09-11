# app/main.py
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from filelock import FileLock
import polars as pl

from app.analytics import (
    compute_event_performance,
    compute_monthly_event_performance,
    compute_monthly_post_performance,
    compute_monthly_student_scores,
    compute_post_performance,
    compute_student_weighted_scores,
    filter_by_days,
    is_monthly_report_day,
    is_weekly_report_day,
)
from app.enrichment import enrich_cache_payload
from app.parser import LogParser
from app.pdf_generator import generate_monthly_pdf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SUMMARIES_DIR = DATA_DIR / "summaries"
CACHE_FILE = DATA_DIR / "analytics_cache.json"
CACHE_LOCK_FILE = DATA_DIR / "analytics_cache.json.lock"
LOCAL_TEST_LOG = DATA_DIR / "hot_tier.log"


def get_aligned_weekly_summaries(max_weeks: int = 4):
    student_map = {f.name.replace("students_", ""): f for f in SUMMARIES_DIR.glob("students_*.parquet")}
    event_map = {f.name.replace("events_", ""): f for f in SUMMARIES_DIR.glob("events_*.parquet")}
    post_map = {f.name.replace("posts_", ""): f for f in SUMMARIES_DIR.glob("posts_*.parquet")}

    all_weeks = sorted(set(student_map.keys()) & set(event_map.keys()) & set(post_map.keys()))
    if not all_weeks:
        return [], [], []

    if len(all_weeks) > max_weeks:
        stale_weeks = all_weeks[:-max_weeks]
        for w in stale_weeks:
            student_map[w].unlink(missing_ok=True)
            event_map[w].unlink(missing_ok=True)
            post_map[w].unlink(missing_ok=True)

    active_weeks = all_weeks[-max_weeks:]
    return [student_map[w] for w in active_weeks], [event_map[w] for w in active_weeks], [post_map[w] for w in active_weeks]


def run_analytics_pipeline():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SUMMARIES_DIR.mkdir(parents=True, exist_ok=True)

    bucket_name = os.getenv("GCP_BUCKET_NAME")
    cleaned_df = None

    if bucket_name:
        logging.info(f"Fetching cloud telemetry logs from bucket: {bucket_name}...")
        cleaned_df = LogParser.load_cloud_telemetry(bucket_name, max_lookback_days=14)
    elif LOCAL_TEST_LOG.exists():
        logging.info(f"Local test mode active. Loading local log file: {LOCAL_TEST_LOG}")
        cleaned_df = LogParser(LOCAL_TEST_LOG).parse_events()
    else:
        logging.critical(f"No GCP_BUCKET_NAME set and local test log file not found at: {LOCAL_TEST_LOG}")
        return

    if cleaned_df is None or cleaned_df.is_empty():
        logging.warning("No telemetry data retrieved. Exiting pipeline.")
        return

    try:
        logging.info("Computing 7-day weekly analytics metrics...")
        weekly_raw_df = filter_by_days(cleaned_df, days=7)
        weekly_students = compute_student_weighted_scores(weekly_raw_df)
        weekly_events = compute_event_performance(weekly_raw_df)
        weekly_posts = compute_post_performance(weekly_raw_df)

        # Deterministic sorting tie-breakers
        if not weekly_students.is_empty():
            sort_cols = [c for c in ["total_score", "weighted_score", "student_id", "user_id"] if c in weekly_students.columns]
            if len(sort_cols) >= 2:
                weekly_students = weekly_students.sort(by=sort_cols[:2], descending=[True, False])

        current_date = datetime.now(timezone.utc)
        iso_year, iso_week, _ = current_date.isocalendar()
        week_key = f"{iso_year}_w{iso_week:02d}"

        if not weekly_students.is_empty() and weekly_students.width > 0:
            weekly_students.write_parquet(SUMMARIES_DIR / f"students_{week_key}.parquet")
        if not weekly_events.is_empty() and weekly_events.width > 0:
            weekly_events.write_parquet(SUMMARIES_DIR / f"events_{week_key}.parquet")
        if not weekly_posts.is_empty() and weekly_posts.width > 0:
            weekly_posts.write_parquet(SUMMARIES_DIR / f"posts_{week_key}.parquet")

        monthly_students_dict, monthly_events_dict, monthly_posts_dict = [], [], []
        post_of_the_month = {}
        existing_cache = {}

        if CACHE_FILE.exists():
            try:
                with FileLock(CACHE_LOCK_FILE, timeout=5):
                    with open(CACHE_FILE, "r", encoding="utf-8") as f:
                        existing_cache = json.load(f)
            except Exception as e:
                logging.warning(f"Could not parse existing cache file: {e}")

        s_files, e_files, p_files = get_aligned_weekly_summaries(max_weeks=4)

        if is_monthly_report_day() or not existing_cache.get("monthly", {}).get("students"):
            if s_files and e_files and p_files:
                m_students_df = compute_monthly_student_scores([pl.read_parquet(f) for f in s_files])
                m_events_df = compute_monthly_event_performance([pl.read_parquet(f) for f in e_files])
                m_posts_df = compute_monthly_post_performance([pl.read_parquet(f) for f in p_files])

                if m_posts_df.width > 0 and not m_posts_df.is_empty():
                    sort_cols = [c for c in ["views", "likes", "post_id", "target_id"] if c in m_posts_df.columns]
                    if sort_cols:
                        m_posts_df = m_posts_df.sort(by=sort_cols, descending=[True] * (len(sort_cols) - 1) + [False])
                    top_posts = m_posts_df.head(1).to_dicts()
                    post_of_the_month = top_posts[0] if top_posts else {}

                monthly_students_dict = m_students_df.to_dicts() if m_students_df.width > 0 else []
                monthly_events_dict = m_events_df.to_dicts() if m_events_df.width > 0 else []
                monthly_posts_dict = m_posts_df.to_dicts() if m_posts_df.width > 0 else []
        else:
            monthly_data = existing_cache.get("monthly", {})
            monthly_students_dict = monthly_data.get("students", [])
            monthly_events_dict = monthly_data.get("events", [])
            monthly_posts_dict = monthly_data.get("posts", [])
            post_of_the_month = monthly_data.get("post_of_the_month", {})

        payload = {
            "last_updated": current_date.isoformat(),
            "student_leaderboard": weekly_students.to_dicts() if weekly_students.width > 0 else [],
            "weekly": {
                "events": weekly_events.to_dicts() if weekly_events.width > 0 else [],
                "posts": weekly_posts.to_dicts() if weekly_posts.width > 0 else [],
            },
            "monthly": {
                "students": monthly_students_dict,
                "events": monthly_events_dict,
                "posts": monthly_posts_dict,
                "post_of_the_month": post_of_the_month,
            },
        }

        try:
            payload = enrich_cache_payload(payload)
        except Exception as e:
            logging.warning(f"Skipping metadata enrichment due to error: {e}")

        # Thread & Process-safe File Lock Cache Writing
        with FileLock(CACHE_LOCK_FILE, timeout=10):
            tmp_cache_path = CACHE_FILE.with_suffix(".tmp")
            with open(tmp_cache_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            tmp_cache_path.replace(CACHE_FILE)

        logging.info(f"Successfully wrote updated cache payload to {CACHE_FILE}")

        if is_monthly_report_day():
            generate_monthly_pdf(
                json_cache_path=str(CACHE_FILE),
                output_pdf=str(DATA_DIR / "Monthly_Executive_Summary.pdf"),
            )

    except Exception as e:
        logging.critical("Fatal error executing telemetry analytics pipeline", exc_info=True)


if __name__ == "__main__":
    run_analytics_pipeline()