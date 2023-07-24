import os
import pandas as pd
import numpy as np
from logger import logger

from tqdm import tqdm
from rich import print

train_folder_path = './train_folder'
test_folder_path = './test_folder'

CORR_THRES = 0.5
MAX_SIM_USER_NUM = 100

def remove_empty_csv_files(csv_folder_path):
    for filename in os.listdir(csv_folder_path):
        if filename.endswith('.csv'):
            csv_file_path = os.path.join(csv_folder_path, filename)
            csv_data = pd.read_csv(csv_file_path, encoding='latin-1')
            if csv_data.empty:
                os.remove(csv_file_path)
                print(f"已删除空的CSV文件：{filename}")

csv_data_list = []
csv_data_dict = {}

def read_files_to_dict(folder_path):
    data_dict = {}
    files = os.listdir(folder_path)
    with tqdm(total=(len(files)), desc=f"Reading files from {folder_path}") as pbar:
        for filename in files:
            if filename.endswith('.csv'):
                csv_file_path = os.path.join(folder_path, filename)
                # 假设你的数据文件中包含了用户ID，你可以根据需要适当修改这里的key
                user_id = filename.replace('user_', '').split('.')[0]
                data_dict[user_id] = pd.read_csv(csv_file_path, index_col=0, encoding='latin-1')
            pbar.update(1)
    return data_dict

logger.info("Read data")
csv_data_dict = read_files_to_dict(train_folder_path)
# 获取test文件夹中的数据字典
test_data_dict = read_files_to_dict(test_folder_path)

all_users = list(test_data_dict.keys())

def value_check(df: pd.DataFrame):
    valid_values = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
    invalid_ratings = df[~df['rating'].isin(valid_values)]
    if not invalid_ratings.empty:
        logger.warning(invalid_ratings)


def corr(userA: str, userB: str, csv_data_dict: dict[str, pd.DataFrame], test_data_dict: dict[str, pd.DataFrame]):
    f1 = test_data_dict.get(userA)  # 从字典中获取用户A的数据
    f2 = csv_data_dict.get(userB)  # 从字典中获取用户B的数据
    f1 = f1.drop_duplicates()
    f2 = f2.drop_duplicates()
    
    # drop 0.0 rating
    f1 = f1[f1['rating'] != 0.0]
    f1.reset_index(drop=True, inplace=True)
    f2 = f2[f2['rating'] != 0.0]
    f2.reset_index(drop=True, inplace=True)
    value_check(f1)
    value_check(f2)

    # find common
    common_albums = pd.merge(f1, f2, how='inner', on=['title', 'artists'])
    if (len(common_albums) < 4):
        return -1

    # print(len(common_albums))

    # cov / (std * std) 
    covar = common_albums['rating_x'].cov(common_albums['rating_y'])
    if covar == 0.0: return 0
    std_x = common_albums['rating_x'].std()
    std_y = common_albums['rating_y'].std()
    # print(covar, std_x, std_y)
    return covar / (std_x * std_y)

def rec_for(n):
    user_id = all_users[n]
    logger.info(f"Recommend music for: {user_id}")
    user_data_A = test_data_dict[user_id]

    mean_rating = user_data_A['rating'].mean()
    # print(mean_rating)
    similar_users = []
    bias = []
    rated_songs_by_user_A = set(zip(user_data_A['title'], user_data_A['artists']))

    logger.info(f"Finding similar users of {user_id}")
    with tqdm(total=(len(csv_data_dict)), desc="Similarity Calculation") as pbar:
        for other_user_id, user_data_other in csv_data_dict.items():
            if other_user_id != user_id and set(zip(user_data_other['title'], user_data_other['artists'])).intersection(
                    rated_songs_by_user_A):
                correlation = corr(user_id, other_user_id, csv_data_dict,test_data_dict)
                if correlation > 0:
                    similar_users.append((other_user_id, correlation))
                    # print(other_user_id)
            pbar.update(1)

    similar_users.sort(key=lambda x: x[1], reverse=True)
    similar_users = similar_users[:MAX_SIM_USER_NUM]

    # similar_users_data = [csv_data_dict[user_id] for user_id in similar_users]
    logger.info(f"Find {len(similar_users)} similar users")
    song_weighted_ratings = {}
    # print(similar_users)

    with tqdm(total=(len(similar_users)), desc="Merging ratings of all similar users") as pbar:
        for user_id_other, correlation in similar_users:
            user_data_other = csv_data_dict[user_id_other]
            for _, row in user_data_other.iterrows():
                song_key = (row['title'], row['artists'])
                if song_key not in song_weighted_ratings:
                    song_weighted_ratings[song_key] = []
                # 将评分乘以对应用户的corr
                song_weighted_ratings[song_key].append((row['rating'] - mean_rating) * correlation)
            pbar.update(1)

    common = rated_songs_by_user_A & set(song_weighted_ratings.keys())
    predict_ratings = {song_key: np.mean(ratings) + mean_rating for song_key, ratings in song_weighted_ratings.items()}
    bias = {
        song_key: predict_ratings[song_key] - user_data_A[user_data_A['title'] == song_key[0]]['rating'].values[0]
        for song_key in common
    }

    # 打印预测的歌曲评分
    # for song_key, pre in predict_ratings.items():
    #     print(f"预测用户{user_id}对歌曲 '{song_key[0]}{song_key[1]}' 的评分： {pre}")

    n = 5  # 推荐歌曲数
    recommended_songs = [song_key for song_key, predict_ratings in
                         sorted(song_weighted_ratings.items(), key=lambda x: x[1], reverse=True) if
                         song_key not in rated_songs_by_user_A][:n]
    logger.info(f"给{user_id}推荐的前 {n} 首歌 {recommended_songs}")
    bias_list = list(bias.values())
    return bias_list

# rec_for(1)

import concurrent.futures
import multiprocessing
from threading import Lock

array_lock = Lock()

total_bias_list = []
with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
    futures = [executor.submit(rec_for, n) for n in range(15)]
    with tqdm(total=(len(futures)), desc="Calculating RMSE") as pbar:
        for future in concurrent.futures.as_completed(futures):
            with array_lock:
                total_bias_list.extend(future.result())
            pbar.update(1)
rmse = np.sqrt(np.mean([bias ** 2 for bias in total_bias_list]))
print(rmse)