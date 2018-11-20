# coding=utf-8
# author=XingLong Pan
# date=2016-11-07

import configparser
import threading

from core.spider import DouBanMovieSpider
from storage import DbHelper

lock = threading.Lock()
parser_lock = threading.Lock()

# 读取配置文件信息
config = configparser.ConfigParser()
config.read('config.ini')

# if database is not empty, use the last movie id in the database as start_id
db_helper = DbHelper.DbHelper()
start_id = db_helper.get_last_movie_id()
db_helper.close_db()
# if database is empty , use the movie id in config.ini as start_id
if not start_id:
    start_id = int(config['common']['start_id'])

spider = DouBanMovieSpider(config, start_id)
spider.start()
spider.join()
