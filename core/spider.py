import random
import threading
from multiprocessing import Queue

import requests

import constants
from page_parser import MovieParser
from proxy import proxy
from storage import DbHelper
from utils import Loggers
from utils import Utils


class DouBanMovieSpider(object):

    def __init__(self, config, start_id):
        self.q = Queue()
        self.q.put(start_id)

        self.logger = Loggers.get_logger(config)
        self.db_helper = DbHelper.DbHelper()
        self.movie_parser = MovieParser.MovieParser()
        self.movie_id_in_queue = set()

        self.proxy = proxy.AbuyunProxy(config)
        self.store_lock = threading.Lock()
        self.parser_lock = threading.Lock()

        self.thread_count = config['spider']['thread_count']
        self.thread_list = []

    def movie_exist_in_db(self, id):
        return self.db_helper.is_movie_id_exists(id)

    def start(self):
        for i_thread in range(self.thread_count):
            t = threading.Thread(target=self.scratch_movie_info(), name='spider thread %d' % i_thread)
            self.thread_list.append(t)
            t.start()

    def join(self):
        for t in self.thread_list:
            t.join()

    def scratch_movie_info(self):
        while True:
            self.logger.debug("Current queue length : " + str(self.q.qsize()))
            try:
                id = self.q.get(timeout=20)
            except:
                self.logger.warning('queue empty, exist thread: %s' % threading.current_thread().name)
                break

            self.logger.debug("Scratch from id : %s in thread : %s" % (id, threading.current_thread().name))
            movie = self.get_movie_by_id(id)

            if not movie:
                self.logger.debug('did not get info from this movie(id=%s)' % id)

                if id in self.movie_id_in_queue:
                    self.movie_id_in_queue.remove(id)

                if not self.proxy.enable:
                    Utils.Utils.delay(constants.DELAY_MIN_SECOND, constants.DELAY_MAX_SECOND)
                continue

            next_movie_ids = movie.get('next_movie_ids', [])

            i = 0
            if self.store_lock.acquire():
                for mid in next_movie_ids:
                        if mid not in self.movie_id_in_queue and not self.movie_exist_in_db(mid):
                            self.movie_id_in_queue.add(id)
                            self.q.put(mid)
                        else:
                            i += 1
                self.store_lock.release()
            if i > 0:
                self.logger.debug('%d movies is already scratched or in the queue.' % i)

            movie['douban_id'] = id
            if self.store_lock.acquire():
                self.db_helper.insert_movie(movie)
                self.store_lock.release()

            if not self.proxy.enable:
                Utils.Utils.delay(constants.DELAY_MIN_SECOND, constants.DELAY_MAX_SECOND)

    def get_movie_by_id(self, id):
        headers = {'User-Agent': random.choice(constants.USER_AGENT)}

        # 获取豆瓣页面(API)数据
        r = None
        while not r:
            try:
                r = requests.get(
                    constants.URL_PREFIX + str(id),
                    headers=headers,
                    # cookies=cookies,
                    proxies=self.proxy
                )
                if not r:
                    break
            except IOError as e:
                self.logger.warning('request exception : %s' % str(e))
                Utils.Utils.delay(1, 5)

        r.encoding = 'utf-8'

        # 提取豆瓣数据
        movie = None
        if self.parser_lock.acquire():
            self.movie_parser.set_html_doc(r.text)
            movie = self.movie_parser.extract_movie_info()
        return movie
