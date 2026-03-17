import requests
import pandas as pd
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.models import Variable


# Use AIRFLOW_HOME so we write data to a location the airflow user can write to in the container
DATA_FOLDER = os.path.join(os.getenv("AIRFLOW_HOME", "/opt/airflow"), "data")
STEAMSPY_API_URL = "https://steamspy.com/api.php?request=top100in2weeks"

def _fetch_and_save_top100_games(**context):
    """Downloads the top 100 games from Steam Spy API, saves them to a local CSV file
    and pushes the file name to XCom for the next task.
    """
    ds = context.get("ds")
    download_datetime = datetime.now()
    
    # Adding headers to look like a real browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    print(f"Downloading data from: {STEAMSPY_API_URL}")
    req = requests.get(STEAMSPY_API_URL, headers=headers)
    
    if req.status_code != 200:
        raise Exception(f"Failed to get top 100 games from Steam Spy: HTTP {req.status_code}")
    
    try:
        data = req.json()
    except Exception as e:
        raise Exception(f"Failed to parse JSON from Steam Spy response: {e}")
    
    # Convert the data to a list of game details
    games_list = []
    for app_id, game_details in data.items():
        # Add app_id to the game details if it's not already there
        if 'appid' not in game_details:
             game_details['appid'] = app_id
        games_list.append(game_details)
        
    # Convert to Pandas DataFrame
    df = pd.DataFrame(games_list)
    print(f"Downloaded {len(df)} games.")

    # Create data directory if it doesn't exist
    os.makedirs(DATA_FOLDER, exist_ok=True)
    
    # Save DataFrame to CSV file
    # ds is a dynamic variable in Airflow, representing the execution date of the DAG
    file_name = f"steamspy_top100_games_{ds}.csv"
    file_path = os.path.join(DATA_FOLDER, file_name)
    df.to_csv(file_path, index=False)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Expected output not found: {file_path}")

    print(f"Data saved to {file_path}")

    ti = context['ti']
    
    return file_name # Return the file name for XCom


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Ensure required Airflow Variables are set at parse time so failures are obvious early.
GCP_BUCKET_NAME = Variable.get('gcp_bucket_name', default_var=None)
if not GCP_BUCKET_NAME:
    raise ValueError("Missing Airflow Variable: gcp_bucket_name (required for GCS upload)")

with DAG(
    dag_id='steamspy_top100_etl',
    default_args=default_args,
    description='Downloads top 100 games from Steam Spy API and uploads to GCS',
    schedule_interval='@daily', # Run daily
    catchup=False,
    tags=['steamspy', 'etl', 'gcp'],
) as dag:

    fetch_and_save_games = PythonOperator(
        task_id='fetch_and_save_top100_games_to_local',
        python_callable=_fetch_and_save_top100_games,
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_top100_games_to_gcs',
        # Fetch the file path from XCom pushed by the previous task
        src="{{ ti.xcom_pull(key='return_value', task_ids='fetch_and_save_top100_games_to_local') | replace('steamspy_top100_games_','/opt/airflow/data/steamspy_top100_games_') }}",
        dst="steamspy/top100_games/{{ ds }}/{{ ti.xcom_pull(key='return_value', task_ids='fetch_and_save_top100_games_to_local') }}",
        bucket=GCP_BUCKET_NAME,  # Use Airflow Variable for bucket name
        gcp_conn_id='google_cloud_default',  # Use the default GCP connection
    )



    fetch_and_save_games >> upload_to_gcs
