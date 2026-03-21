"""
Basic Steam ingestion script: titles, prices, reviews, users.
Supports optional month filtering for review data.
"""



STEAM_API_BASE = "https://api.steampowered.com"
STEAM_STORE_BASE = "https://store.steampowered.com/api"


import requests
import pandas as pd
from datetime import datetime

def get_top100_games():
    download_datetime = datetime.now()
    url = "https://steamspy.com/api.php?request=top100in2weeks"
    
    # Adding headers to look like a real browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    req = requests.get(url, headers=headers)
    
    if req.status_code != 200:
        print(f"Failed to get top 100 games: {req.status_code}")
        return []
    
    try:
        data = req.json()
    except Exception as e:
        print(f"Failed to parse top 100 games: {e}")
        return []
    
    # Convert the data to a list of game details
    games_list = []
    for app_id, game_details in data.items():
        games_list.append(game_details)
        
    return games_list

# Run the function
games = get_top100_games()

def steam_data_ingestion():
    for game in games:
        print(f"Processing game: {game.get('name')} (ID: {game.get('appid')})")
        req = requests.get(f"{STEAM_STORE_BASE}/appdetails?appids={game.get('appid')}")
        if req.status_code != 200:
            print(f"Failed to get details for {game.get('name')}: {req.status_code}")
            continue
        try:
            details = req.json()
            if details and str(game.get('appid')) in details and details[str(game.get('appid'))]['success']:
                game_details = details[str(game.get('appid'))]['data']
                # Here you would typically extract the relevant fields and save them
                print(f"Title: {game_details.get('name')}")
                print(f"Price: {game_details.get('price_overview', {}).get('final_formatted', 'N/A')}")
                print(f"Release Date: {game_details.get('release_date', {}).get('date', 'N/A')}")
                print(f"Metacritic Score: {game_details.get('metacritic', {}).get('score', 'N/A')}")
                print("-" * 40)
            else:
                print(f"No details found for {game.get('name')}")
        except Exception as e:
            print(f"Failed to parse details for {game.get('name')}: {e}")
            continue
    
    # Here you would typically save the data to a database or file
    # For this example, we'll just return the list of games
    return game_details

games_details = pd.DataFrame(steam_data_ingestion())
print(games_details.head())


