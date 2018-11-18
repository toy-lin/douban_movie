# coding=utf-8
# author=XingLong Pan
# date=2016-11-07

import random
import requests
import configparser
import constants
from login import CookiesHelper
from page_parser import MovieParser
from utils import Utils
from storage import DbHelper
from queue import Queue
import random

# 读取配置文件信息
config = configparser.ConfigParser()
config.read('config.ini')

# 获取模拟登录后的cookies
cookie_helper = CookiesHelper.CookiesHelper(
    config['douban']['user'],
    config['douban']['password']
)
cookies = cookie_helper.get_cookies()
print(cookies)

# 实例化爬虫类和数据库连接工具类
movie_parser = MovieParser.MovieParser()
db_helper = DbHelper.DbHelper()

readed_movie_ids = set()

def scratchByQueue(start_id):
    print("Start from id : %s" % start_id)
    q = Queue()
    q.put(start_id)

    while not q.empty():
        print("Current queue length : " + str(q.qsize()))
        id = q.get()
        movie = get_movie_with_id(id)

        if not movie:
            print('did not get info from this movie(id=%s)' % id)
            Utils.Utils.delay(constants.DELAY_MIN_SECOND, constants.DELAY_MAX_SECOND)
            continue
        next_movie_ids = movie['next_movie_ids']
        for mid in next_movie_ids:
            if mid not in readed_movie_ids and not in_db(mid):
                readed_movie_ids.add(mid)
                q.put(mid)
            else:
                print('movie(id=%s) is alread scratched or in the queue.' % mid)

        movie['douban_id'] = id
        db_helper.insert_movie(movie)

        Utils.Utils.delay(constants.DELAY_MIN_SECOND, constants.DELAY_MAX_SECOND)

def in_db(id):
    return db_helper.is_movie_id_exists(id)

def get_last_end_id():
    return db_helper.get_last_movie_id()

def scratchByIteratingID():
    # 读取抓取配置
    START_ID = int(config['common']['start_id'])
    END_ID = int(config['common']['end_id'])
    for i in range(START_ID, END_ID):
        id = str(i)

        movie = get_movie_with_id(id)
        
        # 如果获取的数据为空，延时以减轻对目标服务器的压力,并跳过。
        if not movie:
            Utils.Utils.delay(constants.DELAY_MIN_SECOND, constants.DELAY_MAX_SECOND)
            continue

        # 豆瓣数据有效，写入数据库
        movie['douban_id'] = id
        db_helper.insert_movie(movie)

        Utils.Utils.delay(constants.DELAY_MIN_SECOND, constants.DELAY_MAX_SECOND)

def get_movie_with_id(id):
    headers = {'User-Agent': random.choice(constants.USER_AGENT)}

    # 获取豆瓣页面(API)数据
    r = None
    while not r:
        try:
            r = requests.get(
                constants.URL_PREFIX + id,
                headers=headers,
                cookies=cookies
            )
            if not r:
                break
        except IOError as e:
            print('request exception : %s' % str(e))
            Utils.Utils.delay(5, 10)

    r.encoding = 'utf-8'

    # 提示当前到达的id(log)
    print('scratching movie id: ' + id)

    # 提取豆瓣数据
    movie_parser.set_html_doc(r.text)
    movie = movie_parser.extract_movie_info()
    return movie

# 通过ID进行遍历
start_id = get_last_end_id()
if not start_id:
    start_id = int(config['common']['start_id'])
r_start = int(config['common']['start_id'])
r_end = int(config['common']['end_id'])
for i in range(500):
    print('%d-th round to scratch from movie(id=%s)' % (i,start_id))
    scratchByQueue(start_id)
    start_id = str(random.randrange(r_start,stop=r_end))

# 释放资源
movie_parser = None
db_helper.close_db()