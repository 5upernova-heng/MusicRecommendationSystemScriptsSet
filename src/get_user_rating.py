import os
import time
import requests
import pandas as pd

from datetime import datetime
from typing import List
from bs4 import BeautifulSoup, element
from tqdm import tqdm

from logger import logger

import threading
import concurrent.futures

UNKNOWN_ERROR = -2
REACH_LIMIT = -1
GET_BLOCKED = 0
SUCCESS = 1

visited_folder_name = ""


base = "https://rateyourmusic.com/collection"
visited_lock = threading.Lock()

class UserRatingScraper():
    def __init__(self, user: str, api_key):
        self.user = user
        self.api_key = api_key
        self.visited_file_name = f"./{visited_folder_name}/visited_{user}.txt"
        if not os.path.exists(self.visited_file_name):
            with open(self.visited_file_name, 'w') as f:
                f.close()

    def make_requests(self, url: str):
        """
        Send requests to the server, and get the page
        """
        payload = {'api_key': self.api_key, 'url': url}
        response = requests.request("GET", 'http://api.scraperapi.com', params=payload)
        return response.text

    def get_total_nav(self, first_page: BeautifulSoup) -> int:
        """
        get the total subpage number of the rating page
        """
        navs = first_page.select("a.navlinknum")
        if navs:
            return int(navs[-1].text)
        else:
            return 1

    def get_rows(self, soup: BeautifulSoup) -> List[element.Tag]:
        """
        select all the rows in one rating page
        """
        table_rows = soup.select("tr[id*=page_catalog_item_]")[1:]
        return table_rows

    def parse_one_table_row(self, row: element.Tag) -> pd.DataFrame:
        """
        page -> DataFrame row
        """
        # title: str
        title = row.select_one("a.album").get_text()
        # artists: list
        artists = [artist.get_text() for artist in row.select("a.artist")]
        # tags: list
        tags = [tag.get_text() for tag in row.select(r"a.\35")]
        # rating: float 
        rating = row.select_one("td.or_q_rating_date_s img")
        rating = rating.get('title').split(' ')[0] if rating else 0.0
        # date: str
        return pd.DataFrame(data=[[title, artists, tags, rating]], columns=['title', 'artists', 'tags', 'rating'])

    def save_rows(self, df: pd.DataFrame, file_name: str, table_rows: List[element.Tag]) -> int:
        count = 0
        for row in table_rows:
            count += 1
            df = pd.concat([df, self.parse_one_table_row(row)], ignore_index=True)
        df.to_csv(file_name)
        return count, df

    def add_record(self, url: str):
        with open(self.visited_file_name, 'a') as f:
            f.write(url + '\n')
            f.close()

    def load_records(self) -> List[str]:
        with open(self.visited_file_name, 'r') as f:
            visited = f.readlines()
            visited = [url.replace('\n', '') for url in visited]
            f.close()
        return visited

def get_user_ratings(user_id, api_key):
    csv_file_name = f"./rating_data/user_{user_id}.csv"
    scraper = UserRatingScraper(user_id, api_key)
    if os.path.exists(csv_file_name):
        df = pd.read_csv(csv_file_name, index_col=0)
    else:
        df = pd.DataFrame()
    visited = scraper.load_records()

    rating_base = f"{base}/{user_id}"
    page = scraper.make_requests(rating_base)
    soup = BeautifulSoup(page, 'html.parser')
    nav_num = scraper.get_total_nav(soup)
    for i in tqdm(range(nav_num), desc=f"[{user_id}] [{api_key}] [" + datetime.now().strftime("%H:%M:%S") + "]", position=0, leave=True):
        if i != 0:
            url = f"{rating_base}/{(i + 1)}"
            if url in visited:
                continue
            else:
                visited.append(url)
                scraper.add_record(url)
            page = scraper.make_requests(url)
            soup = BeautifulSoup(page, 'html.parser')
        table_rows = scraper.get_rows(soup)
        if not table_rows:
            if "You've hit the request limit" in soup.text:
                return REACH_LIMIT
            if "We detected multiple users connecting" in soup.text:
                return GET_BLOCKED
            if "authorized request" in soup.text:
                return REACH_LIMIT
            logger.info(soup)
            return UNKNOWN_ERROR
        count, df = scraper.save_rows(df, csv_file_name, table_rows)
    visited.append(rating_base)
    scraper.add_record(rating_base)
    with visited_lock:
        with open("./visited_data/visited.txt", 'a') as f:
            f.write(f"{user_id}\n")
    return SUCCESS


def get_users_ratings_retry_wrapper(user_id):
    result = 0
    while result != SUCCESS:
        api_keys = get_api_keys()
        if len(api_keys) == 0:
            logger.info("[blue]No more api keys, stop")
            return
        api_key = api_keys[0]
        result = get_user_ratings(user_id, api_key)
        if result == GET_BLOCKED:
            logger.info("[blue]Sleep for a minute then retry again")
            time.sleep(60)
        if result == REACH_LIMIT:
            logger.info(f"[blue]API KEY [green]{api_key}[blue] reach limit, change other one")
            move_to_used_out(api_key)
        if result == UNKNOWN_ERROR:
            logger.info("Unknown error, probably website's internal error. Skip this user")
            return
    logger.info(f"{user_id}'s rating all claimed")

    
def get_api_keys():
    with open("./apikeys.txt", 'r') as f:
        keys = f.readlines()
    keys = [key.replace('\n', '') for key in keys]
    return keys
    

def move_to_used_out(api_key):
    """
    move the first line to the used_out
    """
    with open("./apikeys.txt", 'r') as f:
        keys = f.readlines()
    if len(keys) == 0:
        return []
    used_key = keys.pop(0)
    used_key = used_key.replace("\n", '')
    if used_key == api_key:
        logger.info(f"[blue]Move [green]{api_key}[blue] to used_out.txt")
        with open("./apikeys.txt", 'w') as f:
            f.writelines(keys)
        with open("./used_out.txt",'a') as f:
            f.write(f"{used_key}\n")
    return keys


if __name__ == '__main__':
    if not os.path.exists('./visited_data'):
        os.mkdir('./visited_data')
    if not os.path.exists('./rating_data'):
        os.mkdir('./rating_data')
        
    # api_key = input('API_KEY: ')
    # page = input('Page No.')
    with open(f'./userpage_all.txt', 'r') as f:
        users = f.readlines()
        users = [user.replace('\n', '').split('~')[-1] for user in users]
    with open('./visited_data/visited.txt', 'r') as f:
        visited = f.readlines()
        visited = [user.replace('\n', '') for user in visited]
        
    users = [user for user in users if user not in visited] 

    num = len(users)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(get_users_ratings_retry_wrapper, user) for user in users]
        for future in concurrent.futures.as_completed(futures):
            num -= 1
            logger.info(f"Remain: {num} users")
        