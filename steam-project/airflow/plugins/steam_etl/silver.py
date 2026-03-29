"""Silver-layer transformation callables for Steam ETL.

This module is responsible for:
- cleaning bronze Steam API and SteamSpy datasets
- writing silver parquet outputs
- building normalized games/genres/languages parquet tables
"""

import glob
import logging
import os
from datetime import datetime

import pandas as pd
from airflow.exceptions import AirflowException

from steam_etl.io_utils import (
    DATA_FOLDER,
    get_last_processed_date,
    safe_read_csv,
    safe_write_parquet,
    save_last_processed_date,
)

logger = logging.getLogger(__name__)

# Mapping of Steam's internal language codes to readable names
STEAM_LANG_CODE_MAP = {
    "#lang_slovakian": "Slovakian",
}


def transform_steam_api_to_silver(**context):
    """Transform Steam API bronze CSV to silver parquet."""
    ds = context.get("ds") or datetime.now().strftime("%Y-%m-%d")
    task_logger = logger.getChild("transform_steam_api_to_silver")

    file_path = os.path.join(DATA_FOLDER, f"steam_store_details_{ds}.csv")
    df = safe_read_csv(file_path, "steam_api_bronze")
    original_count = len(df)

    expected_columns = [
        "appid",
        "price_usd",
        "price_pln",
        "release_date",
        "languages",
        "genre",
    ]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = pd.Series(dtype="object")

    last_processed_date = get_last_processed_date("steam_api")
    if last_processed_date:
        task_logger.info("Last processed steam_api date: %s", last_processed_date)

    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
    df["release_date_missing"] = df["release_date"].isna()

    # USD uses dot as decimal separator — strip everything except digits, dot, minus
    if "price_usd" in df.columns:
        df["price_usd"] = df["price_usd"].astype(str).str.replace(r"[^\d\.\-]", "", regex=True)
        df["price_usd"] = pd.to_numeric(df["price_usd"], errors="coerce").fillna(0)

    # PLN uses comma as decimal separator (e.g. "74,99zł") — preserve comma, then normalise to dot
    if "price_pln" in df.columns:
        df["price_pln"] = (
            df["price_pln"]
            .astype(str)
            .str.replace(r"[^\d,\-]", "", regex=True)   # keep only digits, comma, minus
            .str.replace(",", ".", regex=False)           # convert decimal comma → dot
        )
        df["price_pln"] = pd.to_numeric(df["price_pln"], errors="coerce").fillna(0)

    # Sanitize language codes: replace Steam's internal #lang_* codes with readable names
    if "languages" in df.columns:
        for code, name in STEAM_LANG_CODE_MAP.items():
            df["languages"] = df["languages"].astype(str).str.replace(code, name, regex=False)

    df = df.drop_duplicates(subset=["appid"], keep="first")
    df["appid"] = df["appid"].astype("int64")

    output_file_name = f"steam_api_silver_{ds}.parquet"
    output_path = os.path.join(DATA_FOLDER, "silver", output_file_name)
    safe_write_parquet(df, output_path, "steam_api_silver")
    save_last_processed_date("steam_api", ds)

    task_logger.info("Steam API silver done: %s -> %s rows", original_count, len(df))
    return output_file_name


def transform_steamspy_to_silver(**context):
    """Transform SteamSpy bronze CSV to silver parquet."""
    ds = context.get("ds") or datetime.now().strftime("%Y-%m-%d")
    task_logger = logger.getChild("transform_steamspy_to_silver")

    file_path = os.path.join(DATA_FOLDER, f"steamspy_top1000_games_{ds}.csv")
    df = safe_read_csv(file_path, "steamspy_bronze")
    # Drop columns that are not needed for analysis or have too many missing values
    cols_to_drop = ["developer", "publisher", "score_rank", "userscore", "price", "initialprice", "discount"]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

    if "ccu" in df.columns:
        df = df.sort_values(by="ccu", ascending=False).reset_index(drop=True)
    # Map owners ranges to categorical labels
    owners_mapping = {
        "5,000,000 .. 10,000,000": "5M-10M",
        "10,000,000 .. 20,000,000": "10M-20M",
        "20,000,000 .. 50,000,000": "20M-50M",
        "50,000,000 .. 100,000,000": "50M-100M",
        "100,000,000 .. 200,000,000": "100M-200M",
        "0 .. 5,000,000": "0-5M",
        "200,000,000 .. ": "200M+",
    }

    if "owners" in df.columns:
        df["owners_category"] = df["owners"].map(owners_mapping)
        df = df.drop(columns=["owners"])

    if "name" in df.columns:
        df["name"] = df["name"].astype("string")
    if "owners_category" in df.columns:
        df["owners_category"] = df["owners_category"].astype("category")

    df["ingestion_date"] = pd.to_datetime(ds)

    numeric_cols = df.select_dtypes(include=["number"]).columns
    for col in ["positive", "negative", "ccu", "average_forever", "average_2weeks", "median_forever", "median_2weeks"]:
        if col in numeric_cols:
            df[col] = df[col].clip(lower=0)

    if "appid" in df.columns:
        df = df.drop_duplicates(subset=["appid"], keep="first")

    output_file_name = f"steamspy_silver_{ds}.parquet"
    output_path = os.path.join(DATA_FOLDER, "silver", output_file_name)
    safe_write_parquet(df, output_path, "steamspy_silver")
    save_last_processed_date("steamspy", ds)

    task_logger.info("SteamSpy silver done: rows=%s", len(df))
    return output_file_name


def transform_to_normalized_tables(**context):
    """Build normalized games, genres, and languages parquet tables."""
    ds = context.get("ds") or datetime.now().strftime("%Y-%m-%d")
    ti = context.get("ti")
    task_logger = logger.getChild("transform_to_normalized_tables")

    api_file_name = ti.xcom_pull(task_ids="transform_steam_api_silver", key="return_value") or f"steam_api_silver_{ds}.parquet"
    silver_folder = os.path.join(DATA_FOLDER, "silver")
    file_path = os.path.join(silver_folder, api_file_name)

    if not os.path.exists(file_path):
        parquet_files = glob.glob(os.path.join(silver_folder, f"steam_api_silver_{ds}*"))
        if not parquet_files:
            raise AirflowException(f"No steam_api silver file found for {ds}")
        file_path = parquet_files[0]

    df = pd.read_parquet(file_path)
    # Create normalized games table by dropping genre and languages, which will be in separate tables
    games = df.drop(columns=["genre", "languages"], errors="ignore").copy()
    safe_write_parquet(games, os.path.join(silver_folder, f"games_{ds}.parquet"), "games_normalized")
    save_last_processed_date("games_normalized", ds)
    # The genre column contains comma-separated values — we need to split and normalize them into a separate table
    if "genre" in df.columns:
        genres = df[["appid", "genre"]].copy()
        genres["genre"] = genres["genre"].str.split(",")
        genres = genres.explode("genre")
        genres["genre"] = genres["genre"].str.strip()
        genres = genres[genres["genre"] != "Free To Play"].reset_index(drop=True)
        safe_write_parquet(genres, os.path.join(silver_folder, f"genres_{ds}.parquet"), "genres_normalized")
        save_last_processed_date("genres_normalized", ds)
    # The languages column contains comma-separated values with potential HTML tags and asterisks indicating audio support. We need to clean and normalize this data.
    if "languages" in df.columns:
        languages = df[["appid", "languages"]].copy()
        languages["language_raw"] = languages["languages"].fillna("").astype(str).str.split(",")
        languages = languages.explode("language_raw").reset_index(drop=True)
        languages = languages.drop(columns=["languages"])
        languages["audio"] = languages["language_raw"].str.contains(r"<strong>\*</strong>|\*", regex=True, na=False)
        languages["language"] = (
            languages["language_raw"]
            .str.replace(r"<br\s*/?>", " ", regex=True)
            .str.replace(r"<strong>\*</strong>", " ", regex=True)
            .str.replace(r"</?[^>]+>", " ", regex=True)
            .str.replace(r"\*", " ", regex=True)
            .str.replace(r"(?i)languages with full audio support", "", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
        languages = languages[
            languages["language"].notna()
            & (languages["language"] != "")
            & (~languages["language"].str.fullmatch(r"(?i)languages with full audio support", na=False))
        ]
        languages = (
            languages.groupby(["appid", "language"], as_index=False)["audio"]
            .max()
            .sort_values(["appid", "language"]) 
            .reset_index(drop=True)
        )
        languages = languages[["appid", "language", "audio"]]
        safe_write_parquet(languages, os.path.join(silver_folder, f"languages_{ds}.parquet"), "languages_normalized")
        save_last_processed_date("languages_normalized", ds)

    task_logger.info("Normalized tables generated for %s", ds)
