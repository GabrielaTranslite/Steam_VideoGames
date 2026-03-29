# Steam Video Games Localization Analytics

Capstone project for Data Engineering Zoomcamp.

This project builds an end-to-end data pipeline that collects Steam game metadata and SteamSpy popularity data, transforms it, and publishes analytics-ready tables in BigQuery.

## 1. Problem Statement

You are an analyst in a localization studio.

You want to understand current trends in game localization and identify possible ways to get new clients.

Main questions this project helps answer:

- Which languages are most common in popular Steam games?
- Which genres show the strongest localization activity?
- How do popularity signals (owners, players, engagement) relate to language coverage?
- Which game segments may represent opportunities for localization services?

## 2. Project Scope

This is a batch pipeline (daily schedule) with the following layers:

- Bronze: raw files from APIs (CSV)
- Silver: cleaned and standardized datasets (Parquet)
- Gold: analytics tables in BigQuery (dbt models)

The project currently ingests top 1000 games from SteamSpy for each run date.

## 3. Tech Stack

- Orchestration: Apache Airflow
- Containerization: Docker + Docker Compose
- Cloud Storage: Google Cloud Storage (GCS)
- Data Warehouse: BigQuery
- Transformations: dbt (`dbt-core`, `dbt-bigquery`)
- Data processing: Python + pandas + pyarrow

## 4. High-Level Architecture

1. Airflow DAG `steamspy_top1000_etl` runs daily.
2. Ingestion tasks pull:
	 - SteamSpy top games
	 - Steam Store details for those appids
3. Raw files are saved locally and uploaded to GCS Bronze.
4. Silver transformations clean and normalize the data to Parquet.
5. Silver Parquet files are uploaded to GCS.
6. dbt prepares external tables in BigQuery and builds Gold marts.
7. Gold tables are ready for BI tools (Tableau section below).

## 5. Repository Structure

Key paths:

- `steam-project/docker-compose.yml`: local Airflow + Postgres stack
- `steam-project/airflow/dags/steamspy_top1000_dag.py`: main orchestration DAG
- `steam-project/airflow/plugins/steam_etl/ingestion.py`: API ingestion logic
- `steam-project/airflow/plugins/steam_etl/silver.py`: Bronze -> Silver transformations
- `steam-project/dbt/`: dbt project for Gold layer
- `steam-project/dbt/models/staging/`: staging models
- `steam-project/dbt/models/marts/`: final analytics models
- `IMPLEMENTATION_NOTES.md`: deeper implementation notes

## 6. Prerequisites

Install:

- Docker Desktop (or Docker Engine + Compose plugin)
- Google Cloud project with BigQuery + GCS enabled

You also need:

- A GCS bucket
- A service account with access to BigQuery and the GCS bucket
- Service account key JSON file

## 7. Configuration

### 7.1 Environment variables

Create or update `steam-project/.env` with your own values.

Required keys:

- `AIRFLOW_UID`
- `AIRFLOW_GID`
- `FERNET_KEY`
- `SECRET_KEY`
- `GCP_BUCKET_NAME`
- `DBT_GOOGLE_PROJECT`
- `DBT_BIGQUERY_DATASET` (default: `steam_gold`)
- `DBT_BIGQUERY_STAGING_DATASET` (default: `steam_staging`)
- `DBT_BIGQUERY_SOURCE_DATASET` (default: `steam_external`)
- `STEAM_API_KEY` (optional for additional Steam Web API usage)

Important:

- Do not commit real secrets to git.
- Use your own `.env` values and your own GCP key.

### 7.2 GCP credentials

Place your service account key file at:

- `steam-project/gcp/credentials.json`

The containers use:

- `GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/gcp/credentials.json`

### 7.3 Airflow variable

The DAG expects Airflow Variable `gcp_bucket_name`.

Set it after containers are running:

```bash
docker compose -f steam-project/docker-compose.yml exec airflow-webserver \
	airflow variables set gcp_bucket_name <your-gcs-bucket>
```

## 8. How To Run (End-to-End)

From repository root:

```bash
cd steam-project
docker compose build
docker compose up -d
```

Initialize Airflow metadata DB:

```bash
docker compose exec airflow-webserver airflow db migrate
```

Create admin user:

```bash
docker compose exec airflow-webserver airflow users create \
	--username admin \
	--firstname Zoomcamp \
	--lastname User \
	--role Admin \
	--email admin@example.com \
	--password admin
```

Set required Airflow variable:

```bash
docker compose exec airflow-webserver airflow variables set gcp_bucket_name <your-gcs-bucket>
```

Open Airflow UI:

- URL: `http://localhost:8080`
- Default login from command above: `admin / admin`

Then:

1. Unpause DAG `steamspy_top1000_etl`
2. Trigger a run (or let schedule run)

## 9. Running dbt Manually (Optional)

If you want to execute dbt manually inside the Airflow container:

```bash
docker compose exec airflow-webserver bash -lc "python /opt/airflow/dbt/scripts/prepare_bigquery_external_tables.py"
docker compose exec airflow-webserver bash -lc "dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --select +path:models/marts"
```

## 10. Expected Outputs

### Bronze (local + GCS)

- SteamSpy raw CSV (top 1000)
- Steam Store details CSV

### Silver (local + GCS)

- `steam_api_silver_<date>.parquet`
- `steamspy_silver_<date>.parquet`
- `games_<date>.parquet`
- `genres_<date>.parquet`
- `languages_<date>.parquet`

### Gold (BigQuery via dbt)

Marts include:

- `fct_games`
- `fct_games_daily` (incremental, partitioned by date, clustered by appid)
- `dim_genres`
- `dim_languages`

## 11. Data Quality and Reliability Notes

- Silver layer includes cleaning, typing, and normalization logic.
- Incremental behavior is tracked with metadata in `silver_metadata.txt`.
- The pipeline uses task retries and file-based logs in Airflow.
- Steam Store requests are throttled in ingestion code to reduce risk of temporary blocking.

## 12. Tableau Dashboard (Placeholder)

I will add Tableau visualizations later.

Planned dashboard content:

- Localization coverage by language and genre
- Trend of language support in popular games over time
- Opportunity view: high-popularity games with lower localization breadth
- Client targeting ideas for localization studio outreach

Suggested Tableau data source:

- BigQuery Gold tables (`steam_gold` dataset), primarily `fct_games_daily`, `fct_games`, `dim_genres`, `dim_languages`

## 13. Reproducibility Checklist

Before sharing your run with other participants, verify:

1. `docker compose ps` shows healthy containers
2. Airflow DAG `steamspy_top1000_etl` finishes successfully
3. New files appear in GCS Bronze and Silver paths
4. dbt models complete without errors
5. BigQuery dataset `steam_gold` contains updated tables

## 14. Known Limitations and Next Steps

- No Terraform/IaC yet (infrastructure setup is manual)
- Tableau dashboard will be added in a later update
- README can later include screenshots of DAG runs and final charts
