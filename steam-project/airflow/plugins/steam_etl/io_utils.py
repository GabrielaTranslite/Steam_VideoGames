"""Shared IO and metadata helpers for bronze-to-silver processing.

This module centralizes:
- local data folder resolution
- incremental metadata reads/writes
- safe CSV/Parquet read-write wrappers with consistent logging/errors
"""

import logging
import os
from typing import Optional

import pandas as pd
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)
DATA_FOLDER = os.path.join(os.getenv("AIRFLOW_HOME", "/opt/airflow"), "data")


def get_last_processed_date(table_name: str) -> Optional[str]:
    """Return last processed YYYY-MM-DD date for a table, or None on first run."""
    try:
        metadata_file = os.path.join(DATA_FOLDER, "silver_metadata.txt")
        if not os.path.exists(metadata_file):
            logger.info("No metadata file yet; first run for %s", table_name)
            return None

        with open(metadata_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith(f"{table_name}="):
                    return line.split("=", 1)[1].strip()
        return None
    except Exception as exc:
        logger.warning("Could not read metadata for %s: %s", table_name, exc)
        return None


def save_last_processed_date(table_name: str, date_str: str) -> bool:
    """Persist last processed date for a table."""
    try:
        metadata_file = os.path.join(DATA_FOLDER, "silver_metadata.txt")
        metadata = {}

        if os.path.exists(metadata_file):
            with open(metadata_file, "r", encoding="utf-8") as f:
                for line in f:
                    if "=" in line:
                        key, val = line.strip().split("=", 1)
                        metadata[key] = val

        metadata[table_name] = date_str
        with open(metadata_file, "w", encoding="utf-8") as f:
            for key, val in metadata.items():
                f.write(f"{key}={val}\n")

        logger.info("Saved metadata %s=%s", table_name, date_str)
        return True
    except Exception as exc:
        logger.error("Failed to save metadata %s=%s: %s", table_name, date_str, exc)
        return False


def safe_read_csv(file_path: str, table_name: str) -> pd.DataFrame:
    """Read CSV with normalized error handling for Airflow tasks."""
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        df = pd.read_csv(file_path)
        logger.info("Read %s: rows=%s cols=%s", table_name, len(df), len(df.columns))
        return df
    except FileNotFoundError as exc:
        raise AirflowException(str(exc)) from exc
    except pd.errors.EmptyDataError as exc:
        raise AirflowException(f"Empty CSV for {table_name}: {exc}") from exc
    except Exception as exc:
        raise AirflowException(f"Failed reading {table_name}: {exc}") from exc


def safe_write_parquet(df: pd.DataFrame, output_path: str, table_name: str) -> bool:
    """Write Parquet with snappy compression and normalized errors."""
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_parquet(output_path, compression="snappy", index=False)
        logger.info("Wrote %s: rows=%s path=%s", table_name, len(df), output_path)
        return True
    except Exception as exc:
        raise AirflowException(f"Failed writing {table_name} parquet: {exc}") from exc
