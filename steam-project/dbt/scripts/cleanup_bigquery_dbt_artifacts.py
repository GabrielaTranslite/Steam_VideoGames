import os

from google.cloud import bigquery


PROJECT_ID = os.getenv("DBT_GOOGLE_PROJECT", "kestra-sandbox-486017")
DATASETS = [
    os.getenv("DBT_BIGQUERY_SOURCE_DATASET", "steam_external"),
    os.getenv("DBT_BIGQUERY_STAGING_DATASET", "steam_staging"),
    os.getenv("DBT_BIGQUERY_DATASET", "steam_gold"),
]

client = bigquery.Client(project=PROJECT_ID)

for dataset_name in DATASETS:
    dataset_id = f"{PROJECT_ID}.{dataset_name}"
    print(f"Cleaning {dataset_id}")
    try:
        tables = list(client.list_tables(dataset_id))
    except Exception as exc:
        print(f"  Skip dataset lookup error: {exc}")
        continue

    if not tables:
        print("  Nothing to delete")
        continue

    for table in tables:
        full_table_id = f"{PROJECT_ID}.{dataset_name}.{table.table_id}"
        client.delete_table(full_table_id, not_found_ok=True)
        print(f"  Deleted {table.table_type}: {full_table_id}")
