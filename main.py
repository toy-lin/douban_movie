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
from multiprocessing import Queue
import random
import logging
import time
import threading

lock = threading.Lock()

logger = logging.getLogger()
logger.setLevel(logging.NOTSET)
rq = time.strftime("%Y-%m-%d %H %M", time.localtime(time.time()))
log_file = '/home/linkaitao/douban.log'
fh = logging.FileHandler(log_file,mode='w')
fh.setLevel(logging.NOTSET)

formatter = logging.Formatter("%(asctime)s -%(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)


# 读取配置文件信息
config = configparser.ConfigParser()
config.read('config.ini')

use_proxy = False
use_mul_thread = False

# 获取模拟登录后的cookies
cookie_helper = CookiesHelper.CookiesHelper(
    config['douban']['user'],
    config['douban']['password']
)
#cookies = cookie_helper.get_cookies()
#logger.debug(cookies)

# 实例化爬虫类和数据库连接工具类
movie_parser = MovieParser.MovieParser()
db_helper = DbHelper.DbHelper()

readed_movie_ids = set()

def scratchByQueue():
    while True:
        logger.debug("Current queue length : " + str(q.qsize()))
        try:
            id = q.get(timeout=20)
        except:
            logger.warn('queue empty, exist thread: %s' % threading.current_thread().name)
            break
        logger.debug("Scratch from id : %s in thread : %s" % (id,threading.current_thread().name))
        movie = get_movie_with_id(id)

        if not movie:
            logger.debug('did not get info from this movie(id=%s)' % id)
            if id in readed_movie_ids:
                readed_movie_ids.remove(id)
            if not use_proxy:
                Utils.Utils.delay(constants.DELAY_MIN_SECOND, constants.DELAY_MAX_SECOND)
            continue
        next_movie_ids = movie.get('next_movie_ids',[])
        for mid in next_movie_ids:
            i = 0
            if lock.acquire(1):
                if mid not in readed_movie_ids and not in_db(mid):
                    readed_movie_ids.add(mid)
                    q.put(mid)
                else:
                    i += 1
                lock.release()
        if i > 0 :
            logger.debug('%d movies is alread scratched or in the queue.' % i)
        movie['douban_id'] = id
        if lock.acquire(1):
            db_helper.insert_movie(movie)
            lock.release()

        if not use_proxy:
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
            if not use_proxy:
                Utils.Utils.delay(constants.DELAY_MIN_SECOND, constants.DELAY_MAX_SECOND)
            continue

        # 豆瓣数据有效，写入数据库
        movie['douban_id'] = id
        db_helper.insert_movie(movie)

        if not use_proxy:
            Utils.Utils.delay(constants.DELAY_MIN_SECOND, constants.DELAY_MAX_SECOND)

def get_movie_with_id(id):
    headers = {'User-Agent': random.choice(constants.USER_AGENT)}

    # 获取豆瓣页面(API)数据
    r = None
    while not r:
        try:
            r = requests.get(
                constants.URL_PREFIX + str(id),
                headers=headers,
                #cookies=cookies,
                proxies=constants.proxies if use_proxy else None
            )
            if not r:
                break
        except IOError as e:
            logger.debug('request exception : %s' % str(e))
            Utils.Utils.delay(5, 10)

    r.encoding = 'utf-8'

    # 提取豆瓣数据
    movie_parser.set_html_doc(r.text)
    movie = movie_parser.extract_movie_info()
    return movie

# 通过ID进行遍历
start_id = get_last_end_id()
if not start_id:
    start_id = int(config['common']['start_id'])

q = Queue()
q.put(start_id)

r_start = int(config['common']['start_id'])
r_end = int(config['common']['end_id'])
for i in range(5 if use_mul_thread else 1):
    t = threading.Thread(target=scratchByQueue,name='scratch thread %d' % i)
    t.start()
    t.join()

# 释放资源
movie_parser = None
db_helper.close_db()