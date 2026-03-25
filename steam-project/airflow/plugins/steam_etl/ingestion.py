"""Bronze-layer ingestion callables for the Steam ETL DAG.

This module contains API download logic only:
- SteamSpy app extraction
- Steam Store enrichment for the downloaded app list
"""

import os
import random
import time
import logging
from datetime import datetime

import pandas as pd
import requests

from steam_etl.io_utils import DATA_FOLDER

logger = logging.getLogger(__name__)

STEAMSPY_API_URL = "https://steamspy.com/api.php"
STEAMSPY_REQUEST = "all"
STEAMSPY_START_PAGE = 0
STEAMSPY_MAX_PAGES = 10
STEAMSPY_TIMEOUT_SECONDS = 30
STEAMSPY_TOP_N = 500
STEAM_STORE_BASE = "https://store.steampowered.com/api"
STEAM_STORE_TIMEOUT_SECONDS = 15
STEAM_STORE_MAX_RETRIES = 5
STEAM_STORE_INTER_REQUEST_SLEEP_SECONDS = 2.0
STEAM_STORE_RETRY_BASE_SLEEP_SECONDS = 2.0
STEAM_STORE_RETRY_MAX_SLEEP_SECONDS = 600.0
STEAM_STORE_BATCH_SIZE = 5
STEAM_STORE_COLUMNS = [
    "appid",
    "price_usd",
    "price_pln",
    "release_date",
    "languages",
    "genre",
]

    # Mapping of Steam's internal language codes to readable names
    STEAM_LANG_CODE_MAP = {
        "#lang_slovakian": "Slovakian",
    }


    def sanitize_languages(languages_str):
        """Convert Steam's internal #lang_* codes to readable language names."""
        if not languages_str:
            return languages_str
    
        result = languages_str
        for code, name in STEAM_LANG_CODE_MAP.items():
            result = result.replace(code, name)
        return result


def fetch_and_save_top500_games(**context):
    """Download SteamSpy appids and store the top N locally as bronze CSV."""
    ds = context.get("ds")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        )
    }

    games_list = []
    seen_appids = set()

    for page in range(STEAMSPY_START_PAGE, STEAMSPY_START_PAGE + STEAMSPY_MAX_PAGES):
        params = {"request": STEAMSPY_REQUEST, "page": page}
        req = requests.get(
            STEAMSPY_API_URL,
            headers=headers,
            params=params,
            timeout=STEAMSPY_TIMEOUT_SECONDS,
        )
        if req.status_code != 200:
            raise Exception(f"Failed to get Steam Spy games: HTTP {req.status_code} page={page}")

        data = req.json()
        if not isinstance(data, dict) or not data:
            break

        for app_id, game_details in data.items():
            if not isinstance(game_details, dict):
                continue

            normalized_appid = game_details.get("appid", app_id)
            try:
                normalized_appid = int(normalized_appid)
            except (TypeError, ValueError):
                continue

            if normalized_appid in seen_appids:
                continue

            game_details["appid"] = normalized_appid
            games_list.append(game_details)
            seen_appids.add(normalized_appid)

            if len(games_list) >= STEAMSPY_TOP_N:
                break

        if len(games_list) >= STEAMSPY_TOP_N:
            break

    if len(games_list) < STEAMSPY_TOP_N:
        logger.warning("SteamSpy returned fewer appids than requested: requested=%s got=%s", STEAMSPY_TOP_N, len(games_list))

    games_list = games_list[:STEAMSPY_TOP_N]

    df = pd.DataFrame(games_list)
    os.makedirs(DATA_FOLDER, exist_ok=True)

    file_name = f"steamspy_top500_games_{ds}.csv"
    file_path = os.path.join(DATA_FOLDER, file_name)
    df.to_csv(file_path, index=False)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Expected output not found: {file_path}")

    context["ti"].xcom_push(key="top500_games", value=games_list)
    return file_name


def fetch_steam_store_details(**context):
    """Enrich top500 app ids with selected Steam Store metadata and save bronze CSV."""
    ds = context.get("ds") or datetime.now().strftime("%Y-%m-%d")
    ti = context.get("ti")
    games_details = ti.xcom_pull(task_ids="fetch_and_save_top500_games_to_local", key="top500_games") or []

    file_name = f"steam_store_details_{ds}.csv"
    file_path = os.path.join(DATA_FOLDER, file_name)

    if not games_details:
        os.makedirs(DATA_FOLDER, exist_ok=True)
        pd.DataFrame(columns=STEAM_STORE_COLUMNS).to_csv(file_path, index=False)
        return file_name

    appids = []
    for game in games_details:
        raw_appid = game.get("appid")
        if raw_appid is None:
            continue
        try:
            appids.append(int(raw_appid))
        except (TypeError, ValueError):
            continue

    total_appids = len(appids)
    total_batches = (total_appids + STEAM_STORE_BATCH_SIZE - 1) // STEAM_STORE_BATCH_SIZE
    logger.info(
        "Steam Store enrichment starting: appids=%s batch_size=%s total_batches=%s",
        total_appids,
        STEAM_STORE_BATCH_SIZE,
        total_batches,
    )

    def chunked(values, size):
        for idx in range(0, len(values), size):
            yield values[idx : idx + size]

    def throttled_sleep():
        sleep_for = STEAM_STORE_INTER_REQUEST_SLEEP_SECONDS + random.uniform(2.0, 5.0)
        logger.info("Steam Store throttle sleep: %.2fs", sleep_for)
        time.sleep(sleep_for)

    def fetch_store_batch(appid_batch, cc: str, batch_idx: int):
        appids_param = ",".join(str(appid) for appid in appid_batch)
        store_url = f"{STEAM_STORE_BASE}/appdetails?appids={appids_param}&cc={cc}&l=en"

        # Steam Store heavily rate-limits burst traffic; retry with backoff on 429/5xx.
        for attempt in range(STEAM_STORE_MAX_RETRIES + 1):
            try:
                resp = requests.get(store_url, timeout=STEAM_STORE_TIMEOUT_SECONDS)
            except requests.RequestException:
                resp = None

            if resp is not None and resp.status_code == 200:
                try:
                    payload = resp.json()
                except ValueError:
                    return {}

                out = {}
                for appid in appid_batch:
                    app_payload = payload.get(str(appid), {})
                    if app_payload.get("success") and isinstance(app_payload.get("data"), dict):
                        out[appid] = app_payload["data"]
                return out

            status = resp.status_code if resp is not None else None
            if status == 400:
                # Some Steam Store environments reject multi-appid requests; split and retry in smaller chunks.
                if len(appid_batch) == 1:
                    return {}

                mid = len(appid_batch) // 2
                left = fetch_store_batch(appid_batch[:mid], cc, batch_idx)
                right = fetch_store_batch(appid_batch[mid:], cc, batch_idx)
                left.update(right)
                return left

            if status not in {429, 500, 502, 503, 504}:
                return {}

            sleep_for = min(STEAM_STORE_RETRY_MAX_SLEEP_SECONDS, STEAM_STORE_RETRY_BASE_SLEEP_SECONDS * (2 ** attempt))

            if status == 429 and resp is not None:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        sleep_for = max(sleep_for, float(retry_after))
                    except ValueError:
                        pass

            logger.info(
                "Steam Store retry: batch=%s/%s region=%s attempt=%s status=%s sleep=%.2fs",
                batch_idx,
                total_batches,
                cc,
                attempt + 1,
                status,
                sleep_for,
            )
            time.sleep(sleep_for)

        logger.warning(
            "Steam Store batch failed after retries: batch=%s/%s region=%s size=%s",
            batch_idx,
            total_batches,
            cc,
            len(appid_batch),
        )
        return {}

    rows = []
    processed_any = 0
    for batch_idx, appid_batch in enumerate(chunked(appids, STEAM_STORE_BATCH_SIZE), start=1):
        logger.info(
            "Processing Steam Store batch %s/%s: size=%s first_appid=%s last_appid=%s",
            batch_idx,
            total_batches,
            len(appid_batch),
            appid_batch[0],
            appid_batch[-1],
        )

        us_batch = fetch_store_batch(appid_batch, "us", batch_idx)
        throttled_sleep()
        pl_batch = fetch_store_batch(appid_batch, "pl", batch_idx)
        throttled_sleep()

        batch_any = sum(1 for appid in appid_batch if appid in us_batch or appid in pl_batch)
        processed_any += batch_any
        logger.info(
            "Batch %s/%s results: us_success=%s pl_success=%s any_success=%s cumulative_any_success=%s",
            batch_idx,
            total_batches,
            len(us_batch),
            len(pl_batch),
            batch_any,
            processed_any,
        )

        for appid in appid_batch:
            game_details = us_batch.get(appid)
            game_details_pl = pl_batch.get(appid)
            if not game_details and not game_details_pl:
                continue

            price_usd = game_details.get("price_overview", {}).get("final_formatted") if game_details else None
            price_pln = game_details_pl.get("price_overview", {}).get("final_formatted") if game_details_pl else None

            metadata = game_details or game_details_pl
            release_date = metadata.get("release_date", {}).get("date")

            languages = metadata.get("supported_languages") or metadata.get("languages")
            genres = metadata.get("genres") or []
            genre = ", ".join([g.get("description", "") for g in genres]) if isinstance(genres, list) else genres

            rows.append(
                {
                    "appid": appid,
                    "price_usd": price_usd,
                    "price_pln": price_pln,
                    "release_date": release_date,
                    "languages": sanitize_languages(languages),
                    "genre": genre,
                }
            )

    df = pd.DataFrame(rows, columns=STEAM_STORE_COLUMNS)
    logger.info("Steam Store enrichment complete: output_rows=%s input_appids=%s", len(df), total_appids)
    os.makedirs(DATA_FOLDER, exist_ok=True)

    df.to_csv(file_path, index=False)
    return file_name
