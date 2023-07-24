from bs4 import BeautifulSoup
import requests
import datetime
import re
import pandas as pd
import time
from lxml import etree


for a in range(1):
    url = 'https://rateyourmusic.com/charts/top/album/all-time/g:ambient/{}'.format(a+1)
    header = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"}
    resp = requests.get(url, headers=header).text
    html = etree.HTML(resp)
    li_list = html.xpath("/html/body/div[6]/div/div[2]/sections/group[2]/section[3]/div") 
    for li in li_list:
        href = li.xpath('.//div[2]/div[1]/div[1]/div[1]/a/@href')
        for href in href:
            complete_url = 'https://rateyourmusic.com{}'.format(href)
            print(complete_url)
            with open('li.txt', 'a', encoding='utf-8') as f:
                f.write(complete_url)
                f.write('\n')