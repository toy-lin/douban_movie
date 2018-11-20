# coding=utf-8
# author=XingLong Pan
# date=2016-11-07

import configparser
import threading

from core.spider import DouBanMovieSpider
from login import CookiesHelper
from storage import DbHelper
from utils import Loggers

lock = threading.Lock()
parser_lock = threading.Lock()

# 读取配置文件信息
config = configparser.ConfigParser()
config.read('config.ini')

# 默认的日志输出
logger = Loggers.get_logger(config)

# 获取模拟登录后的cookies
cookie_helper = CookiesHelper.CookiesHelper(
    config['douban']['user'],
    config['douban']['password']
)

# 模拟用户登录
# cookies = cookie_helper.get_cookies()
# logger.debug(cookies)

# 实例化爬虫类和数据库连接工具类
db_helper = DbHelper.DbHelper()
start_id = db_helper.get_last_movie_id()
db_helper.close_db()

if not start_id:
    start_id = int(config['common']['start_id'])

spider = DouBanMovieSpider(config, start_id)
spider.start()
spider.join()