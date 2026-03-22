"""SteamSpy ETL DAG orchestration.

This DAG coordinates three stages:
1) bronze ingestion (API pulls -> local CSV -> GCS bronze)
2) silver transformations (cleaning + parquet outputs)
3) normalized table generation (games/genres/languages parquet)

Business logic lives in plugin modules under plugins/steam_etl.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from steam_etl.ingestion import fetch_and_save_top100_games, fetch_steam_store_details
from steam_etl.io_utils import upload_file_to_gcs
from steam_etl.silver import (
    transform_steam_api_to_silver,
    transform_steamspy_to_silver,
    transform_to_normalized_tables,
)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 3, 21),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

GCP_BUCKET_NAME = Variable.get("gcp_bucket_name", default_var=None)
if not GCP_BUCKET_NAME:
    raise ValueError("Missing Airflow Variable: gcp_bucket_name (required for GCS upload)")

with DAG(
    dag_id="steamspy_top100_etl",
    default_args=default_args,
    description="SteamSpy + Steam Store ingestion, silver transforms, and normalized outputs",
    schedule_interval="0 12 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["steamspy", "etl", "gcp"],
) as dag:

    fetch_and_save_games = PythonOperator(
        task_id="fetch_and_save_top100_games_to_local",
        python_callable=fetch_and_save_top100_games,
    )

    ingest_steam_store_details = PythonOperator(
        task_id="fetch_steam_store_details_to_local",
        python_callable=fetch_steam_store_details,
    )

    upload_steamspy_to_gcs = PythonOperator(
        task_id="upload_steamspy_top100_to_gcs",
        python_callable=upload_file_to_gcs,
        op_kwargs={
            "src": "{{ ti.xcom_pull(key='return_value', task_ids='fetch_and_save_top100_games_to_local') | replace('steamspy_top100_games_','/opt/airflow/data/steamspy_top100_games_') }}",
            "dst": "bronze/SteamSpy/{{ ds }}/{{ ti.xcom_pull(key='return_value', task_ids='fetch_and_save_top100_games_to_local') }}",
            "bucket": GCP_BUCKET_NAME,
            "mime_type": "text/csv",
        },
    )

    upload_steamapi_to_gcs = PythonOperator(
        task_id="upload_steamapi_store_details_to_gcs",
        python_callable=upload_file_to_gcs,
        op_kwargs={
            "src": "{{ ti.xcom_pull(key='return_value', task_ids='fetch_steam_store_details_to_local') | replace('steam_store_details_','/opt/airflow/data/steam_store_details_') }}",
            "dst": "bronze/SteamAPI/{{ ds }}/{{ ti.xcom_pull(key='return_value', task_ids='fetch_steam_store_details_to_local') }}",
            "bucket": GCP_BUCKET_NAME,
            "mime_type": "text/csv",
        },
    )

    transform_steam_api = PythonOperator(
        task_id="transform_steam_api_silver",
        python_callable=transform_steam_api_to_silver,
    )

    transform_steamspy = PythonOperator(
        task_id="transform_steamspy_silver",
        python_callable=transform_steamspy_to_silver,
    )

    normalize_tables = PythonOperator(
        task_id="normalize_steam_api_tables",
        python_callable=transform_to_normalized_tables,
    )

    run_dbt_gold = BashOperator(
        task_id="run_dbt_gold_models",
        env={
            "DBT_EXTERNAL_TABLE_MODE": "latest",
            "DBT_EXTERNAL_TABLE_DATE": "{{ ds }}",
        },
        append_env=True,
        bash_command=(
            "python /opt/airflow/dbt/scripts/prepare_bigquery_external_tables.py && "
            "dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --select +path:models/marts"
        ),
    )

    upload_steam_api_silver = PythonOperator(
        task_id="upload_steam_api_silver_to_gcs",
        python_callable=upload_file_to_gcs,
        op_kwargs={
            "src": "/opt/airflow/data/silver/steam_api_silver_{{ ds }}.parquet",
            "dst": "silver/SteamAPI/{{ ds }}/steam_api_silver_{{ ds }}.parquet",
            "bucket": GCP_BUCKET_NAME,
            "mime_type": "application/octet-stream",
        },
    )

    upload_steamspy_silver = PythonOperator(
        task_id="upload_steamspy_silver_to_gcs",
        python_callable=upload_file_to_gcs,
        op_kwargs={
            "src": "/opt/airflow/data/silver/steamspy_silver_{{ ds }}.parquet",
            "dst": "silver/SteamSpy/{{ ds }}/steamspy_silver_{{ ds }}.parquet",
            "bucket": GCP_BUCKET_NAME,
            "mime_type": "application/octet-stream",
        },
    )

    upload_games_table = PythonOperator(
        task_id="upload_games_table_to_gcs",
        python_callable=upload_file_to_gcs,
        op_kwargs={
            "src": "/opt/airflow/data/silver/games_{{ ds }}.parquet",
            "dst": "silver/Normalized/{{ ds }}/games_{{ ds }}.parquet",
            "bucket": GCP_BUCKET_NAME,
            "mime_type": "application/octet-stream",
        },
    )

    upload_genres_table = PythonOperator(
        task_id="upload_genres_table_to_gcs",
        python_callable=upload_file_to_gcs,
        op_kwargs={
            "src": "/opt/airflow/data/silver/genres_{{ ds }}.parquet",
            "dst": "silver/Normalized/{{ ds }}/genres_{{ ds }}.parquet",
            "bucket": GCP_BUCKET_NAME,
            "mime_type": "application/octet-stream",
        },
    )

    upload_languages_table = PythonOperator(
        task_id="upload_languages_table_to_gcs",
        python_callable=upload_file_to_gcs,
        op_kwargs={
            "src": "/opt/airflow/data/silver/languages_{{ ds }}.parquet",
            "dst": "silver/Normalized/{{ ds }}/languages_{{ ds }}.parquet",
            "bucket": GCP_BUCKET_NAME,
            "mime_type": "application/octet-stream",
        },
    )

    fetch_and_save_games >> [upload_steamspy_to_gcs, ingest_steam_store_details]
    ingest_steam_store_details >> upload_steamapi_to_gcs

    upload_steamapi_to_gcs >> transform_steam_api
    upload_steamspy_to_gcs >> transform_steamspy

    transform_steam_api >> normalize_tables

    transform_steam_api >> upload_steam_api_silver
    transform_steamspy >> upload_steamspy_silver
    normalize_tables >> [upload_games_table, upload_genres_table, upload_languages_table]

    [transform_steam_api, transform_steamspy, normalize_tables] >> run_dbt_gold