# %%
import pandas as pd
import requests

from rich import print
from tqdm import tqdm

base = "https://rateyourmusic.com/charts/top/album/all-time/deweight:live,archival,soundtrack"

# %%
def make_requests(url: str) -> str:
    """
    Send requests to the server, and get the page
    """
    payload = {'api_key': "63f2bafb5350ea35aba15ae3b692f1b3", 'url': url}
    response = requests.request("GET", 'http://api.scraperapi.com', params=payload)
    return response.text

# %%
from bs4 import BeautifulSoup

def parse_album_links(page):
    soup = BeautifulSoup(page, 'html.parser')
    musics = soup.select(".page_charts_section_charts_item_image_link")
    links = [music.get('href') for music in musics]
    return links

# %%
def parse_album_infos(page):
    soup = BeautifulSoup(page, 'html.parser')
    # title
    title = soup.select_one(".album_title")
    title = title.contents[0].text.strip() if title else title
    # artist
    artists = list(set([artist.text.strip() for artist in soup.select(".section_main_info .artist")]))
    # released
    released = soup.select_one(".section_main_info tr:nth-child(3) td")
    released = released.text.strip() if released else released
    # Average
    avg_rating = soup.select_one(".avg_rating")
    avg_rating = avg_rating.text.strip() if avg_rating else avg_rating
    # Rating num
    num_ratings = soup.select_one(".num_ratings b")
    num_ratings = num_ratings.text.strip() if num_ratings else num_ratings
    # genres
    generes = [genre.text.strip() for genre in soup.select(".release_pri_genres a")]
    # descriptors
    descriptors = soup.select_one(".release_pri_descriptors")
    descriptors = descriptors.text.strip() if descriptors else descriptors
    # language
    language = soup.select_one(".section_main_info tr:nth-child(9) td")
    language = language.text.strip() if language else language
    return pd.DataFrame(data=[[title, artists, released, avg_rating, num_ratings, generes, descriptors, language]], 
                        columns=['title', 'artists', 'released', 'avg_rating', 'num_ratings', 'genres', 'descriptors', 'language'])


def add_record(visited_file_name, url: str):
    with open(visited_file_name, 'a') as f:
        f.write(url + '\n')

def load_records(visited_file_name):
    with open(visited_file_name, 'r') as f:
        visited = f.readlines()
        visited = [url.replace('\n', '') for url in visited]
        f.close()
    return visited

# %%
def parse_one_page(index):
    if os.path.exists(f"./albums/{index}.csv"):
        df = pd.read_csv(f"./albums/{index}.csv", index_col=0)
        # print(df.size)
        if df.size == 360:
            print(f"{index} exists, skip")
            return
    else:
        df = pd.DataFrame()

    url = f"{base}/{index}"
    visited_file_path = f"./visited_data/visited_list_{index}.txt"
    if os.path.exists(visited_file_path):
        visited = load_records(visited_file_path)
    else:
        visited = []
    page = make_requests(url)
    links = parse_album_links(page)
    length = len(links)
    # print(f"Parsed {length} at index {index}")
    with tqdm(total=(length), desc=f"Index {index}", position=0, leave=True) as pbar:
        for link in links:
            try:
                if link in visited:
                    print(f"{link} parsed, skip")
                    continue
                album_page = make_requests(f"https://rateyourmusic.com/{link}")
                info = parse_album_infos(album_page)
                df = pd.concat([df, info], ignore_index=True)
                visited.append(link)
                add_record(visited_file_path, link)
            except Exception as e:
                print(e)
            pbar.update(1)
    df.to_csv(f"./albums/{index}.csv")
    print(f"Complete: {index}")
    # print(f"Save to ./albums/{index}.csv")

# %%
import concurrent.futures
import os

if not os.path.exists("./albums"):
    os.mkdir("./albums")

with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(parse_one_page, index + 1) for index in range(125)]
    # with tqdm(total=(len(futures)), desc="<<[TOTAL PROGRESS]>>:", position=0) as pbar:
    for future in concurrent.futures.as_completed(futures):
        ...