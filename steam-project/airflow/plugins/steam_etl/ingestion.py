"""Bronze-layer ingestion callables for the Steam ETL DAG.

This module contains API download logic only:
- SteamSpy top100 extraction
- Steam Store enrichment for the downloaded app list
"""

import os
from datetime import datetime

import pandas as pd
import requests

from .io_utils import DATA_FOLDER

STEAMSPY_API_URL = "https://steamspy.com/api.php?request=top100in2weeks"
STEAM_STORE_BASE = "https://store.steampowered.com/api"


def fetch_and_save_top100_games(**context):
    """Download SteamSpy top 100 and store as local bronze CSV."""
    ds = context.get("ds")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        )
    }

    req = requests.get(STEAMSPY_API_URL, headers=headers)
    if req.status_code != 200:
        raise Exception(f"Failed to get top 100 games from Steam Spy: HTTP {req.status_code}")

    data = req.json()

    games_list = []
    for app_id, game_details in data.items():
        if "appid" not in game_details:
            game_details["appid"] = app_id
        games_list.append(game_details)

    df = pd.DataFrame(games_list)
    os.makedirs(DATA_FOLDER, exist_ok=True)

    file_name = f"steamspy_top100_games_{ds}.csv"
    file_path = os.path.join(DATA_FOLDER, file_name)
    df.to_csv(file_path, index=False)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Expected output not found: {file_path}")

    context["ti"].xcom_push(key="top100_games", value=games_list)
    return file_name


def fetch_steam_store_details(**context):
    """Enrich top100 app ids with Steam Store metadata and save bronze CSV."""
    ds = context.get("ds") or datetime.now().strftime("%Y-%m-%d")
    ti = context.get("ti")
    games_details = ti.xcom_pull(task_ids="fetch_and_save_top100_games_to_local", key="top100_games") or []

    if not games_details:
        return None

    rows = []
    for game in games_details:
        appid = game.get("appid")

        def fetch_store_data(cc: str):
            store_url = f"{STEAM_STORE_BASE}/appdetails?appids={appid}&cc={cc}&l=en"
            resp = requests.get(store_url)
            if resp.status_code != 200:
                return None

            resp_details = resp.json()
            if not resp_details or str(appid) not in resp_details or not resp_details[str(appid)].get("success"):
                return None
            return resp_details[str(appid)]["data"]

        game_details = fetch_store_data("us")
        game_details_pl = fetch_store_data("pl")
        if not game_details and not game_details_pl:
            continue

        price_usd = game_details.get("price_overview", {}).get("final_formatted") if game_details else None
        price_pln = game_details_pl.get("price_overview", {}).get("final_formatted") if game_details_pl else None

        metadata = game_details or game_details_pl
        release_date = metadata.get("release_date", {}).get("date")
        metacritic_score = metadata.get("metacritic", {}).get("score")

        languages = game_details.get("supported_languages") or game_details.get("languages")
        genres = game_details.get("genres") or []
        genre = ", ".join([g.get("description", "") for g in genres]) if isinstance(genres, list) else genres

        developer = game_details.get("developers")
        if isinstance(developer, list):
            developer = ", ".join(developer)

        publisher = game_details.get("publishers")
        if isinstance(publisher, list):
            publisher = ", ".join(publisher)

        series_data = game_details.get("series")
        series = ", ".join(series_data) if isinstance(series_data, list) else series_data

        rows.append(
            {
                "appid": appid,
                "title": metadata.get("name"),
                "price_usd": price_usd,
                "price_pln": price_pln,
                "release_date": release_date,
                "metacritic_score": metacritic_score,
                "languages": languages,
                "genre": genre,
                "developer": developer,
                "publisher": publisher,
                "series": series,
            }
        )

    if not rows:
        return None

    df = pd.DataFrame(rows)
    os.makedirs(DATA_FOLDER, exist_ok=True)

    file_name = f"steam_store_details_{ds}.csv"
    file_path = os.path.join(DATA_FOLDER, file_name)
    df.to_csv(file_path, index=False)
    return file_name
