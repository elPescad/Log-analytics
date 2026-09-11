from datetime import datetime, timedelta, timezone
import gzip
import polars as pl

# Point Weights Configuration
WEIGHT_ATTEND = 50  # Actual QR scan check-in
WEIGHT_POST = 5
WEIGHT_LIKE = 1


# --- INGESTION & PARSING HELPERS ---


def parse_raw_gcs_log(gz_bytes: bytes) -> pl.DataFrame:
    """Decompresses .gz bytes from GCS and flattens JSON payload to match metric schemas."""
    try:
        try:
            decompressed = gzip.decompress(gz_bytes)
        except (gzip.BadGzipFile, OSError):
            decompressed = gz_bytes

        if not decompressed or not decompressed.strip():
            return pl.DataFrame()

        df = pl.read_ndjson(decompressed)
        if df.is_empty() or "payload" not in df.columns:
            return pl.DataFrame()

        cleaned_df = df.select([
            pl.col("timestamp").alias("server_timestamp"),
            pl.col("payload").struct.field("u").alias("user_id"),
            pl.col("payload").struct.field("e").alias("event_type"),
            pl.col("payload").struct.field("id").alias("target_id"),
            pl.col("payload").struct.field("yr").alias("college_year"),
        ])

        return cleaned_df
    except Exception as e:
        print(f"Failed to parse GCS log payload: {e}")
        return pl.DataFrame()


# --- TIME WINDOW HELPERS ---


def filter_by_days(cleaned_df: pl.DataFrame, days: int) -> pl.DataFrame:
    """Filters logs to only include entries from the last N days based on server_timestamp."""
    if cleaned_df.is_empty() or "server_timestamp" not in cleaned_df.columns:
        return cleaned_df

    now_seconds = int(datetime.now(timezone.utc).timestamp())
    cutoff_seconds = now_seconds - (days * 86400)

    max_ts = cleaned_df["server_timestamp"].max()
    if max_ts and max_ts > 1e11:
        cutoff_epoch = cutoff_seconds * 1000
    else:
        cutoff_epoch = cutoff_seconds

    return cleaned_df.filter(pl.col("server_timestamp") >= cutoff_epoch)


def is_weekly_report_day() -> bool:
    """Returns True if today is Friday (weekday 4)."""
    return datetime.now(timezone.utc).weekday() == 4


def is_monthly_report_day() -> bool:
    """Returns True if today is the 1st of the month."""
    return datetime.now(timezone.utc).day == 1


# --- RAW LOG AGGREGATION ENGINES (WEEKLY) ---


def compute_student_weighted_scores(cleaned_df: pl.DataFrame) -> pl.DataFrame:
    required_cols = {"user_id", "college_year", "target_id", "event_type"}
    if cleaned_df.is_empty() or not required_cols.issubset(cleaned_df.columns):
        return pl.DataFrame()

    return (
        cleaned_df.filter(pl.col("user_id").is_not_null())
        .group_by("user_id")
        .agg(
            pl.col("college_year").drop_nulls().last().alias("college_year"),
            pl.col("target_id")
            .filter(
                (pl.col("event_type") == "qr_scan_success")
                & pl.col("target_id").is_not_null()
            )
            .drop_nulls()
            .n_unique()
            .alias("attend_count"),
            pl.col("target_id")
            .filter(
                (pl.col("event_type") == "qr_scan_success")
                & pl.col("target_id").is_not_null()
            )
            .drop_nulls()
            .unique()
            .alias("attended_event_ids"),
            pl.col("event_type")
            .filter(pl.col("event_type") == "create_post_start")
            .count()
            .alias("post_count"),
            pl.col("event_type")
            .filter(pl.col("event_type") == "like")
            .count()
            .alias("like_count"),
        )
        .with_columns(pl.col("college_year").fill_null("Alumni"))
        .with_columns(
            (pl.col("attend_count") * WEIGHT_ATTEND).alias("attend_pts"),
            (pl.col("post_count") * WEIGHT_POST).alias("post_pts"),
            (pl.col("like_count") * WEIGHT_LIKE).alias("like_pts"),
        )
        .with_columns(
            (
                pl.col("attend_pts") + pl.col("post_pts") + pl.col("like_pts")
            ).alias("total_score")
        )
        # Sort by total_score descending, with user_id ascending as explicit tie-breaker
        .sort(by=["total_score", "user_id"], descending=[True, False])
    )


def compute_event_performance(cleaned_df: pl.DataFrame) -> pl.DataFrame:
    required_cols = {"target_id", "event_type", "user_id"}
    if cleaned_df.is_empty() or not required_cols.issubset(cleaned_df.columns):
        return pl.DataFrame()

    return (
        cleaned_df.filter(pl.col("target_id").is_not_null())
        .filter(
            pl.col("event_type").is_in(
                ["view_event", "qr_scan_success", "rsvp_success"]
            )
        )
        .group_by("target_id")
        .agg(
            pl.col("event_type")
            .filter(pl.col("event_type") == "view_event")
            .count()
            .alias("views"),
            pl.col("event_type")
            .filter(pl.col("event_type") == "rsvp_success")
            .count()
            .alias("total_rsvps"),
            pl.col("user_id")
            .filter(
                (pl.col("event_type") == "qr_scan_success")
                & pl.col("user_id").is_not_null()
            )
            .drop_nulls()
            .n_unique()
            .alias("actual_attended"),
            pl.col("user_id")
            .filter(
                (pl.col("event_type") == "qr_scan_success")
                & pl.col("user_id").is_not_null()
            )
            .drop_nulls()
            .unique()
            .alias("attendee_user_ids"),
        )
        .with_columns(
            pl.col("actual_attended").alias("attended"),
            pl.when(pl.col("actual_attended") > 0)
            .then((pl.col("total_rsvps") / pl.col("actual_attended")).round(2))
            .otherwise(0.0)
            .alias("rsvp_to_attendance_ratio"),
            pl.when(pl.col("total_rsvps") > 0)
            .then(
                (
                    pl.col("actual_attended") / pl.col("total_rsvps") * 100
                ).round(1)
            )
            .otherwise(0.0)
            .alias("turnout_vs_rsvp_pct"),
        )
        .with_columns(pl.col("turnout_vs_rsvp_pct").alias("conversion_rate_pct"))
        # Sort by actual_attended desc, views desc, target_id asc tie-breaker
        .sort(
            by=["actual_attended", "views", "target_id"],
            descending=[True, True, False],
        )
    )


def compute_post_performance(cleaned_df: pl.DataFrame) -> pl.DataFrame:
    required_cols = {"target_id", "event_type"}
    if cleaned_df.is_empty() or not required_cols.issubset(cleaned_df.columns):
        return pl.DataFrame()

    return (
        cleaned_df.filter(pl.col("target_id").is_not_null())
        .filter(pl.col("event_type").is_in(["view_post", "like", "view_comments"]))
        .group_by("target_id")
        .agg(
            pl.col("event_type")
            .filter(pl.col("event_type") == "view_post")
            .count()
            .alias("views"),
            pl.col("event_type")
            .filter(pl.col("event_type") == "like")
            .count()
            .alias("likes"),
            pl.col("event_type")
            .filter(pl.col("event_type") == "view_comments")
            .count()
            .alias("comment_views"),
        )
        .with_columns(
            pl.when(pl.col("views") > 0)
            .then(
                (
                    (pl.col("likes") + pl.col("comment_views"))
                    / pl.col("views")
                    * 100
                ).round(1)
            )
            .otherwise(0.0)
            .alias("engagement_rate_pct")
        )
        # Sort by views desc, likes desc, target_id asc tie-breaker
        .sort(
            by=["views", "likes", "target_id"],
            descending=[True, True, False],
        )
    )


# --- SUMMARY ROLLUP ENGINES (MONTHLY) ---


def _get_valid_dfs(
    dfs: list[pl.DataFrame], required_cols: set[str] = None
) -> list[pl.DataFrame]:
    valid = []
    for df in dfs:
        if df.is_empty():
            continue
        if required_cols and not required_cols.issubset(df.columns):
            continue
        valid.append(df)
    return valid


def compute_monthly_post_performance(
    weekly_post_dfs: list[pl.DataFrame],
) -> pl.DataFrame:
    required_cols = {"target_id", "views", "likes", "comment_views"}
    valid_dfs = _get_valid_dfs(weekly_post_dfs, required_cols)
    if not valid_dfs:
        return pl.DataFrame()

    return (
        pl.concat(valid_dfs)
        .group_by("target_id")
        .agg(
            pl.col("views").sum(),
            pl.col("likes").sum(),
            pl.col("comment_views").sum(),
        )
        .with_columns(
            pl.when(pl.col("views") > 0)
            .then(
                (
                    (pl.col("likes") + pl.col("comment_views"))
                    / pl.col("views")
                    * 100
                ).round(1)
            )
            .otherwise(0.0)
            .alias("engagement_rate_pct")
        )
        # Deterministic sorting sequence for monthly posts
        .sort(
            by=["engagement_rate_pct", "views", "likes", "target_id"],
            descending=[True, True, True, False],
        )
    )


def compute_monthly_student_scores(
    weekly_student_dfs: list[pl.DataFrame],
) -> pl.DataFrame:
    required_cols = {
        "user_id",
        "college_year",
        "attended_event_ids",
        "post_count",
        "like_count",
    }
    valid_dfs = _get_valid_dfs(weekly_student_dfs, required_cols)
    if not valid_dfs:
        return pl.DataFrame()

    return (
        pl.concat(valid_dfs)
        .group_by("user_id")
        .agg(
            pl.col("college_year").drop_nulls().last().alias("college_year"),
            pl.col("attended_event_ids")
            .list.explode()
            .drop_nulls()
            .unique()
            .alias("attended_event_ids"),
            pl.col("post_count").sum(),
            pl.col("like_count").sum(),
        )
        .with_columns(
            pl.col("college_year").fill_null("Alumni"),
            pl.col("attended_event_ids").list.len().alias("attend_count"),
        )
        .with_columns(
            (pl.col("attend_count") * WEIGHT_ATTEND).alias("attend_pts"),
            (pl.col("post_count") * WEIGHT_POST).alias("post_pts"),
            (pl.col("like_count") * WEIGHT_LIKE).alias("like_pts"),
        )
        .with_columns(
            (
                pl.col("attend_pts") + pl.col("post_pts") + pl.col("like_pts")
            ).alias("total_score")
        )
        # Deterministic sorting sequence for monthly students
        .sort(by=["total_score", "user_id"], descending=[True, False])
    )


def compute_monthly_event_performance(
    weekly_event_dfs: list[pl.DataFrame],
) -> pl.DataFrame:
    required_cols = {"target_id", "views", "total_rsvps", "attendee_user_ids"}
    valid_dfs = _get_valid_dfs(weekly_event_dfs, required_cols)
    if not valid_dfs:
        return pl.DataFrame()

    return (
        pl.concat(valid_dfs)
        .group_by("target_id")
        .agg(
            pl.col("views").sum(),
            pl.col("total_rsvps").sum(),
            pl.col("attendee_user_ids")
            .list.explode()
            .drop_nulls()
            .unique()
            .alias("attendee_user_ids"),
        )
        .with_columns(
            pl.col("attendee_user_ids").list.len().alias("actual_attended")
        )
        .with_columns(
            pl.col("actual_attended").alias("attended"),
            pl.when(pl.col("actual_attended") > 0)
            .then((pl.col("total_rsvps") / pl.col("actual_attended")).round(2))
            .otherwise(0.0)
            .alias("rsvp_to_attendance_ratio"),
            pl.when(pl.col("total_rsvps") > 0)
            .then(
                (
                    pl.col("actual_attended") / pl.col("total_rsvps") * 100
                ).round(1)
            )
            .otherwise(0.0)
            .alias("turnout_vs_rsvp_pct"),
        )
        .with_columns(pl.col("turnout_vs_rsvp_pct").alias("conversion_rate_pct"))
        # Deterministic sorting sequence for monthly events
        .sort(
            by=["actual_attended", "views", "target_id"],
            descending=[True, True, False],
        )
    )