# Imports and Helper functions

from datetime import datetime
import time
import requests
import pickle
from pathlib import Path
import re

def print_log(*args):
    print(f"[{str(datetime.now())[:-3]}] ", end="")
    print(*args)
    
def get_search_results(params):
    req_sr = requests.get(
        "https://store.steampowered.com/search/results/",
        params=params)
    
    if req_sr.status_code != 200:
        print_log(f"Failed to get search results: {req_sr.status_code}")
        return {"items": []}
    
    try:
        search_results = req_sr.json()
    except Exception as e:
        print_log(f"Failed to parse search results: {e}")
        return {"items": []}
    
    return search_results
    
def get_app_details(appid):
    while(True):
        if appid == None:
            print_log("App Id is None.")
            return {}

        appdetails_req = requests.get(
            "https://store.steampowered.com/api/appdetails/",
            params={"appids": appid, "cc": "hk", "l": "english"})        # change the countrycode to the region you are staying with
        
        if appdetails_req.status_code == 200:
            appdetails = appdetails_req.json()
            appdetails = appdetails[str(appid)]
            print_log(f"App Id: {appid} - {appdetails['success']}")
            break

        elif appdetails_req.status_code == 429:
            print_log(f'Too many requests. Sleep for 10 sec')
            time.sleep(10)
            continue

        elif appdetails_req.status_code == 403:
            print_log(f'Forbidden to access. Sleep for 5 min.')
            time.sleep(5 * 60)
            continue

        else:
            print_log("ERROR: status code:", appdetails_req.status_code)
            print_log(f"Error in App Id: {appid}.")
            appdetails = {}
            break

    return appdetails

    # Main code

execute_datetime = datetime.now()

search_result_folder_path = Path(f"search_results_{execute_datetime.strftime('%Y%m%d')}")
if not search_result_folder_path.exists():
    search_result_folder_path.mkdir()
    
# a list of filters
params_list = [
    {"filter": "topsellers"},
    {"filter": "globaltopsellers"},
    {"filter": "popularnew"},
    {"filter": "popularcommingsoon"},
    {"filter": "", "specials": 1}
]
page_list = list(range(1, 5))

params_sr_default = {
    "filter": "topsellers",
    "hidef2p": 1,
    "page": 1,            # page is used to go through different parts of the ranking. Each page contains 25 results
    "json": 1
}

for update_param in params_list:

    items_all = []
    if update_param["filter"]:
        filename = f"{update_param['filter']}_{execute_datetime.strftime('%Y%m%d')}.pkl"
    else:
        filename = f"specials_{execute_datetime.strftime('%Y%m%d')}.pkl"

    if (search_result_folder_path / filename).exists():
        print_log(f"File {filename} exists. Skip.")
        continue

    for page_no in page_list:
        param = params_sr_default.copy()
        param.update(update_param)
        param["page"] = page_no

        search_results = get_search_results(param)
        print_log(search_results)

        if not search_results:
            continue

        items = search_results.get("items", [])

        # proprocessing search results to retrieve the appid of the game
        for item in items:
            try:
                item["appid"] = re.search(r"steam/\w+/(\d+)", item["logo"]).group(1)      # the URL can be steam/bundles/{appid} or steam/apps/{appid}
            except Exception as e:
                print_log(f"Failed to extract appid: {e}")
                item["appid"] = None

        # request for game information using appid
        for item in items:
            appid = item["appid"]
            appdetails = get_app_details(appid)
            item["appdetail"] = appdetails

        items_all.extend(items)

    # save the search results
    with open(search_result_folder_path / filename, "wb") as f:
        pickle.dump(items_all, f)
    print_log(f"Saved {filename}")