import os
from typing import Iterable, List

from google.cloud import bigquery, storage


PROJECT_ID = os.getenv("DBT_GOOGLE_PROJECT", "kestra-sandbox-486017")
DATASET_ID = os.getenv("DBT_BIGQUERY_SOURCE_DATASET", "steam_external")
BUCKET_NAME = os.getenv("DBT_GCS_BUCKET", "steam-project")
LOCATION = os.getenv("DBT_BIGQUERY_LOCATION", "US")
TABLE_MODE = os.getenv("DBT_EXTERNAL_TABLE_MODE", "all").strip().lower()
TABLE_DATE = os.getenv("DBT_EXTERNAL_TABLE_DATE", "").strip()

TABLE_SPECS = {
    "steam_api_raw": {
        "prefix": "silver/SteamAPI/",
        "needle": "steam_api_silver_",
    },
    "steamspy_raw": {
        "prefix": "silver/SteamSpy/",
        "needle": "steamspy_silver_",
    },
    "games_normalized": {
        "prefix": "silver/Normalized/",
        "needle": "games_",
    },
    "genres_normalized": {
        "prefix": "silver/Normalized/",
        "needle": "genres_",
    },
    "languages_normalized": {
        "prefix": "silver/Normalized/",
        "needle": "languages_",
    },
}


def list_matching_uris(storage_client: storage.Client, prefix: str, needle: str) -> List[str]:
    uris = []
    for blob in storage_client.list_blobs(BUCKET_NAME, prefix=prefix):
        if blob.name.endswith(".parquet") and needle in blob.name:
            uris.append(f"gs://{BUCKET_NAME}/{blob.name}")
    if not uris:
        raise RuntimeError(f"No parquet files found for prefix={prefix!r}, needle={needle!r} in bucket {BUCKET_NAME!r}")
    return sorted(uris)


def latest_uri(storage_client: storage.Client, prefix: str, needle: str) -> List[str]:
    uris = list_matching_uris(storage_client, prefix, needle)
    return [uris[-1]]


def dated_uris(storage_client: storage.Client, prefix: str, needle: str, table_date: str) -> List[str]:
    uris = list_matching_uris(storage_client, prefix, needle)
    matched = [uri for uri in uris if table_date in uri]
    if not matched:
        raise RuntimeError(
            f"No parquet files found for date={table_date!r}, prefix={prefix!r}, needle={needle!r} in bucket {BUCKET_NAME!r}"
        )
    return matched


def uris_for_table(storage_client: storage.Client, prefix: str, needle: str) -> List[str]:
    if TABLE_DATE:
        return dated_uris(storage_client, prefix, needle, TABLE_DATE)
    if TABLE_MODE == "latest":
        return latest_uri(storage_client, prefix, needle)
    if TABLE_MODE == "all":
        return list_matching_uris(storage_client, prefix, needle)
    raise ValueError("DBT_EXTERNAL_TABLE_MODE must be one of: all, latest")


def quote_uris(uris: Iterable[str]) -> str:
    return ", ".join(f"'{uri}'" for uri in uris)


storage_client = storage.Client(project=PROJECT_ID)
bigquery_client = bigquery.Client(project=PROJECT_ID)

dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
dataset_ref.location = LOCATION
bigquery_client.create_dataset(dataset_ref, exists_ok=True)

for table_name, spec in TABLE_SPECS.items():
    uris = uris_for_table(storage_client, spec["prefix"], spec["needle"])
    sql = f"""
    create or replace external table `{PROJECT_ID}.{DATASET_ID}.{table_name}`
    options (
      format = 'PARQUET',
      uris = [{quote_uris(uris)}]
    )
    """
    bigquery_client.query(sql).result()
    print(
        f"Prepared external table {PROJECT_ID}.{DATASET_ID}.{table_name} "
        f"with {len(uris)} file(s) in mode={TABLE_MODE}"
    )
