import time
import gzip
import io
import logging
from pathlib import Path
import polars as pl
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError
from app.models import TelemetryRegistry

logger = logging.getLogger(__name__)

class LogParser:

    def __init__(self, log_source: str | Path | io.StringIO | io.BytesIO):
        self.log_source = log_source

    def load_raw(self) -> pl.DataFrame:
        """Loads raw NDJSON safely from a local file path or an in-memory stream."""
        try:
            if isinstance(self.log_source, (io.StringIO, io.BytesIO)):
                # Ensure we are reading from the start of the stream
                self.log_source.seek(0)
                return pl.read_ndjson(self.log_source)
            
            path = Path(self.log_source)
            if not path.exists():
                logger.error(f"Cannot find log file at {path}")
                return pl.DataFrame()
                
            return pl.read_ndjson(path)
        except pl.exceptions.NoDataError:
            logger.warning("Encountered empty NDJSON file/stream. Skipping.")
            return pl.DataFrame()
        except Exception as e:
            logger.error(f"Failed to load raw NDJSON: {e}")
            return pl.DataFrame()

    def parse_events(self) -> pl.DataFrame:
        df = self.load_raw()

        if df.is_empty():
            return pl.DataFrame()

        # Defend against schema drift: Ensure required top-level columns exist
        required_cols = ["payload", "timestamp"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            logger.warning(f"Skipping malformed segment missing columns: {missing_cols}")
            return pl.DataFrame()

        payload_schema = pl.Struct(
            {
                "e": pl.String,   # Event type
                "id": pl.String,  # Target ID
                "t": pl.Int64,    # Metric value
                "u": pl.String,   # User ID
                "yr": pl.String   # College Year
            }
        )

        try:
            payload_dtype = df.schema["payload"]

            # Safely handle both JSON string payloads and native Polars Structs
            if payload_dtype in (pl.String, pl.Utf8):
                payload_expr = pl.col("payload").str.json_decode(dtype=payload_schema)
            elif isinstance(payload_dtype, pl.Struct):
                payload_expr = pl.col("payload").cast(payload_schema)
            else:
                payload_expr = pl.col("payload").cast(pl.String).str.json_decode(dtype=payload_schema)

            result = (
                df.with_columns(payload_expr.alias("payload"))
                .unnest("payload")
                .rename(
                    {
                        "timestamp": "server_timestamp", 
                        "e": "event_type", 
                        "id": "target_id", 
                        "t": "metric_value",
                        "u": "user_id",
                        "yr": "college_year"
                    }
                )
            )

            return result.filter(pl.col("event_type").is_not_null())
        except Exception as e:
            logger.error(f"Error during dataframe schema transformation: {e}")
            return pl.DataFrame()

    def organize_events(self, cleaned_df: pl.DataFrame) -> TelemetryRegistry:
        if cleaned_df.is_empty():
            # Thanks to our hardened dataclass, this safely initializes empty dfs!
            return TelemetryRegistry()

        # Defend against missing event_type column
        if "event_type" not in cleaned_df.columns:
            logger.error("Cannot organize events: 'event_type' column missing.")
            return TelemetryRegistry(other=cleaned_df)

        post_events_type = [
            "view_post", "view_comments", "like", "unlike", 
            "delete_post", "create_post_start", "create_post_cancel"
        ]

        event_rsvp_types = [
            "view_event", "rsvp_click", "rsvp_success", 
            "rsvp_cancel", "share_event", "qr_scan_success"
        ]

        feed_ui_types = [
            "pull_to_refresh", "scroll_depth", 
            "check_rank", "profile_press"
        ]

        session_types = ["session_start", "session_end"]

        posts_df = cleaned_df.filter(pl.col("event_type").is_in(post_events_type))
        events_df = cleaned_df.filter(pl.col("event_type").is_in(event_rsvp_types))
        feed_df = cleaned_df.filter(pl.col("event_type").is_in(feed_ui_types))
        session_df = cleaned_df.filter(pl.col("event_type").is_in(session_types))

        all_known = post_events_type + event_rsvp_types + feed_ui_types + session_types
        other_df = cleaned_df.filter(~pl.col("event_type").is_in(all_known))

        return TelemetryRegistry(
            posts=posts_df,
            events=events_df,
            feed_ui=feed_df,
            sessions=session_df,
            other=other_df
        )

    @staticmethod
    def load_cloud_telemetry(bucket_name: str, max_lookback_days: int = 14) -> pl.DataFrame:
        """
        Fetches GCP log segments created within max_lookback_days,
        skipping older or corrupted files entirely to optimize memory and ensure pipeline stability.
        """
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix="segment_")
        except GoogleCloudError as e:
            logger.error(f"Failed to connect to GCP Bucket {bucket_name}: {e}")
            return pl.DataFrame()

        cutoff_timestamp = int(time.time()) - (max_lookback_days * 86400)
        dfs = []
        
        for blob in blobs:
            if not blob.name.endswith(".gz"):
                continue

            try:
                # Extract timestamp safely
                file_timestamp = int(blob.name.split("_")[1].split(".")[0])
                if file_timestamp < cutoff_timestamp:
                    continue
            except (IndexError, ValueError):
                logger.warning(f"Malformed blob name, skipping time check: {blob.name}")
                pass

            # Fault-tolerant download and decompression block
            try:
                compressed_bytes = blob.download_as_bytes()
                decompressed_bytes = gzip.decompress(compressed_bytes)
                
                parser = LogParser(io.BytesIO(decompressed_bytes))
                parsed_df = parser.parse_events()
                
                if not parsed_df.is_empty():
                    dfs.append(parsed_df)
                    
            except gzip.BadGzipFile:
                logger.error(f"Corrupted GZIP file skipped: {blob.name}")
            except Exception as e:
                logger.error(f"Failed to process blob {blob.name}: {e}")

        if not dfs:
            logger.warning("No valid telemetry data parsed from cloud.")
            return pl.DataFrame()

        # how="diagonal" protects against minor schema variations between chunks
        return pl.concat(dfs, how="diagonal", rechunk=True)