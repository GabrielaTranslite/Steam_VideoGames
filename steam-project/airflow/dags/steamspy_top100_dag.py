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
STEAM_API_BASE = "https://api.steampowered.com"
STEAM_STORE_BASE = "https://store.steampowered.com/api"


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
    ti.xcom_push(key='top100_games', value=games_list)

    return file_name  # Return the file name for XCom

# Defining a function to fetch additional data for each game using Steam API

def steam_data_ingestion(**context):
    """Fetch detailed Steam Store metadata for each game and save it to CSV.

    This task pulls the top 100 Steam Spy game list from XCom (pushed by
    `_fetch_and_save_top100_games`) and then enriches each entry with Steam Store
    metadata.
    """
    ds = context.get("ds") or datetime.now().strftime("%Y-%m-%d")
    ti = context.get("ti")
    games_details = ti.xcom_pull(task_ids='fetch_and_save_top100_games_to_local', key='top100_games') or []

    if not games_details:
        print("No game list found in XCom; skipping Steam Store ingestion.")
        return None

    rows = []
    for game in games_details:
        appid = game.get('appid')
        name = game.get('name')
        print(f"Processing game: {name} (ID: {appid})")
        def fetch_store_data(cc: str):
            store_url = f"{STEAM_STORE_BASE}/appdetails?appids={appid}&cc={cc}&l=en"
            resp = requests.get(store_url)
            if resp.status_code != 200:
                print(f"Failed to get {cc.upper()} details for {name}: {resp.status_code}")
                return None

            try:
                resp_details = resp.json()
            except Exception as e:
                print(f"Failed to parse JSON for {name} ({cc.upper()}): {e}")
                return None

            if not resp_details or str(appid) not in resp_details or not resp_details[str(appid)].get('success'):
                print(f"No {cc.upper()} details found for {name}")
                return None

            return resp_details[str(appid)]['data']

        game_details = fetch_store_data('us')
        game_details_pl = fetch_store_data('pl')
        if not game_details and not game_details_pl:
            continue

        price_usd = None
        price_pln = None
        if game_details:
            price_usd = game_details.get('price_overview', {}).get('final_formatted')
        if game_details_pl:
            price_pln = game_details_pl.get('price_overview', {}).get('final_formatted')

        # Use metadata from the US request if available, otherwise fall back to PL.
        metadata = game_details or game_details_pl
        release_date = metadata.get('release_date', {}).get('date')
        metacritic_score = metadata.get('metacritic', {}).get('score')

        # The store API has a few different ways to represent language data.
        languages = game_details.get('supported_languages') or game_details.get('languages')

        genres = game_details.get('genres') or []
        genre = ", ".join([g.get('description', '') for g in genres]) if isinstance(genres, list) else genres

        developer = game_details.get('developers')
        if isinstance(developer, list):
            developer = ", ".join(developer)

        publisher = game_details.get('publishers')
        if isinstance(publisher, list):
            publisher = ", ".join(publisher)

        series_data = game_details.get('series')
        if isinstance(series_data, list):
            series = ", ".join(series_data)
        else:
            series = series_data

        rows.append({
            "appid": appid,
            "title": metadata.get('name'),
            "price_usd": price_usd,
            "price_pln": price_pln,
            "release_date": release_date,
            "metacritic_score": metacritic_score,
            "languages": languages,
            "genre": genre,
            "developer": developer,
            "publisher": publisher,
            "series": series,
        })

    if not rows:
        print("No Steam Store data collected; skipping CSV write.")
        return None

    df = pd.DataFrame(rows)
    os.makedirs(DATA_FOLDER, exist_ok=True)

    file_name = f"steam_store_details_{ds}.csv"
    file_path = os.path.join(DATA_FOLDER, file_name)
    df.to_csv(file_path, index=False)

    print(f"Saved Steam Store details for {len(df)} games to {file_path}")

    return file_name

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

    ingest_steam_store_details = PythonOperator(
        task_id='fetch_steam_store_details_to_local',
        python_callable=steam_data_ingestion,
    )

    upload_steamspy_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_steamspy_top100_to_gcs',
        # Fetch the file path from XCom pushed by the previous task
        src="{{ ti.xcom_pull(key='return_value', task_ids='fetch_and_save_top100_games_to_local') | replace('steamspy_top100_games_','/opt/airflow/data/steamspy_top100_games_') }}",
        dst="bronze/SteamSpy/{{ ds }}/{{ ti.xcom_pull(key='return_value', task_ids='fetch_and_save_top100_games_to_local') }}",
        bucket=GCP_BUCKET_NAME,  # Use Airflow Variable for bucket name
        gcp_conn_id='google_cloud_default',  # Use the default GCP connection
    )

    upload_steamapi_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_steamapi_store_details_to_gcs',
        # Fetch the file path from XCom pushed by the Steam Store ingestion task
        src="{{ ti.xcom_pull(key='return_value', task_ids='fetch_steam_store_details_to_local') | replace('steam_store_details_','/opt/airflow/data/steam_store_details_') }}",
        dst="bronze/SteamAPI/{{ ds }}/{{ ti.xcom_pull(key='return_value', task_ids='fetch_steam_store_details_to_local') }}",
        bucket=GCP_BUCKET_NAME,
        gcp_conn_id='google_cloud_default',
    )

    fetch_and_save_games >> upload_steamspy_to_gcs
    fetch_and_save_games >> ingest_steam_store_details >> upload_steamapi_to_gcs
