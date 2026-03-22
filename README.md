# Steam_VideoGames
Capstone project for Data Engineering Zoomcamp - released Video Games on Steam and their languages

## dbt integration

The project now includes a dbt layer under `steam-project/dbt`.

- Adapter: `dbt-bigquery`
- Gold layer target: BigQuery dataset `steam_gold`
- Silver input: parquet files uploaded by Airflow to GCS
- External source dataset: BigQuery dataset `steam_external` over GCS parquet files, prepared by a helper script before dbt build

Main dbt marts:

- `fct_games`
- `dim_genres`
- `dim_languages`

Airflow DAG `steamspy_top100_etl` includes task `run_dbt_gold_models` that executes:

- `python /opt/airflow/dbt/scripts/prepare_bigquery_external_tables.py`
- `dbt build --select +path:models/marts`
