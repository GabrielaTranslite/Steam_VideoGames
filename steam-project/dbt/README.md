# dbt in steam-project

This dbt project builds a Gold layer in BigQuery from silver parquet files stored in GCS.

## Architecture

- Silver parquet files are produced by Airflow and uploaded to GCS.
- Helper script `scripts/prepare_bigquery_external_tables.py` creates BigQuery external tables over those GCS parquet files in dataset `steam_external`.
- dbt staging models build views in dataset `steam_staging`.
- dbt marts build Gold tables in dataset `steam_gold`, including an incremental daily fact table.

## External table mode

- `DBT_EXTERNAL_TABLE_MODE=all` (default): external tables include all matching silver parquet files and support historical/incremental dbt models.
- `DBT_EXTERNAL_TABLE_MODE=latest`: external tables point only to the newest file (useful for quick latest-state runs).
- `DBT_EXTERNAL_TABLE_DATE=YYYY-MM-DD`: when set, external tables point only to files for that specific snapshot date. Airflow should set this to `{{ ds }}` for deterministic reruns and backfills.

## Profile and Engine

- Adapter: BigQuery
- Auth: service account from `/opt/airflow/gcp/credentials.json`
- Profile file: `profiles.yml` in this folder

## Models

- `models/staging/stg_steam_api.sql`
- `models/staging/stg_steamspy.sql`
- `models/staging/stg_languages.sql`
- `models/marts/fct_games_daily.sql` (incremental append by `snapshot_date`)
- `models/marts/fct_games.sql`
- `models/marts/dim_genres.sql`
- `models/marts/dim_languages.sql`

## Run inside airflow container

```bash
python /opt/airflow/dbt/scripts/prepare_bigquery_external_tables.py
dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --select +path:models/marts
```

## Airflow integration

DAG task `run_dbt_gold_models` runs dbt after silver parquet files are uploaded to GCS.
For scheduled Airflow runs, the task sets `DBT_EXTERNAL_TABLE_MODE=latest` and `DBT_EXTERNAL_TABLE_DATE={{ ds }}` so dbt reads only the parquet files for the DAG run date. This avoids schema conflicts with older historical parquet files and prevents reruns from accidentally reading a newer day's empty snapshot.
