import random
import threading
from queue import Queue

import requests

import constants
from login import CookiesHelper
from page_parser import MovieParser
from proxy import proxy
from storage import DbHelper
from utils import Loggers
from utils import Utils


class DouBanMovieSpider(object):

    def __init__(self, config, start_id):
        # a dict of cookies
        self.cookies = None
        self.thread_count = int(config['spider']['thread_count'])
        self.network_max_try_times = int(config['network']['max_try_times'])

        self.movie_id_in_queue = set()
        self.queue = Queue()
        self.queue.put(start_id)
        self.movie_id_in_queue.add(start_id)

        self.logger = Loggers.get_logger(config)
        self.db_helper = DbHelper.DbHelper()

        self.login_if_necessary(config)
        

        self.proxy = proxy.AbuyunProxy(config)
        self.store_lock = threading.Lock()
        self.db_lock = threading.Lock()

        self.thread_list = []

    def movie_exist_in_db(self, id):
        return self.db_helper.is_movie_id_exists(id)

    def start(self):
        for i_thread in range(self.thread_count):
            t = threading.Thread(target=self.scratch_movie_info, name='spider thread %d' % i_thread)
            self.thread_list.append(t)
            t.start()

    def join(self):
        for t in self.thread_list:
            t.join()

    def scratch_movie_info(self):
        while True:
            self.logger.debug("Current queue length : " + str(self.queue.qsize()))
            id_scratch = None
            try:
                id_scratch = self.queue.get(timeout=20)
            except:
                self.logger.warning('queue empty, exist thread: %s' % threading.current_thread().name)
                break

            self.logger.debug("Scratch from id : %s in thread : %s" % (id_scratch, threading.current_thread().name))
            movie = self.get_movie_by_id(id_scratch)

            if not movie:
                self.logger.debug('did not get info from this movie(id=%s)' % id_scratch)
                self.queue.put(id_scratch)

                if not self.proxy.enable:
                    Utils.Utils.delay(constants.DELAY_MIN_SECOND, constants.DELAY_MAX_SECOND)
                continue

            next_movie_ids = movie.get('next_movie_ids', [])

            if len(next_movie_ids) > 0 :
                if self.store_lock.acquire():
                    for mid in next_movie_ids:
                        if mid in self.movie_id_in_queue:
                            continue
                        self.logger.debug('add %s to queue(len=%d)' % (mid,len(self.movie_id_in_queue)))
                        self.movie_id_in_queue.add(mid)
                        self.queue.put(mid)
                    self.store_lock.release()

            if self.db_lock.acquire():
                self.db_helper.insert_movie(movie)
                self.db_lock.release()

            # if proxy is enable , we won't wait for seconds because the proxy will change IP very frequently
            if not self.proxy.enable:
                Utils.Utils.delay(constants.DELAY_MIN_SECOND, constants.DELAY_MAX_SECOND)

    def get_movie_by_id(self, id):
        # change headers every time, it seems unnecessary when we use proxy
        headers = {'User-Agent': random.choice(constants.USER_AGENT)}

        # 获取豆瓣页面(API)数据
        r = None
        try_times = 0
        while not r:
            try:
                try_times += 1
                r = requests.get(
                    constants.URL_PREFIX + str(id),
                    headers=headers,
                    cookies=self.cookies,
                    proxies=self.proxy.get()
                )

                if not r:
                    if try_times <= self.network_max_try_times:
                        # wait seconds if we can't get any response
                        Utils.Utils.delay(1, 5)
                    else:
                        self.logger.error('Cannot get movie(id=%s) info' % id)
                        return None
            except IOError as e:
                self.logger.warning('request exception : %s' % str(e))
                if try_times <= self.network_max_try_times:
                    # wait for seconds if any network error represent, there must be some troubles with the network
                    Utils.Utils.delay(1, 5)
                else:
                    self.logger.error('Cannot get movie(id=%s) info' % id)
                    return None

        r.encoding = 'utf-8'

        # 提取电影数据
        movie_parser = MovieParser.MovieParser()
        movie_parser.set_html_doc(r.text)
        movie = movie_parser.extract_movie_info()
        if movie:
            movie['douban_id'] = id
        return movie

    def login_if_necessary(self, config):
        login_enable = int(config['login_douban']['enable']) == 1
        if not login_enable:
            self.logger.debug('douban login enable : %s' % str(login_enable))
            return

        cookie_helper = CookiesHelper.CookiesHelper(
            config['douban']['user'],
            config['douban']['password']
        )
        # 模拟用户登录
        self.cookies = cookie_helper.get_cookies()
        self.logger.debug('cookies: %s' % str(self.cookies))
