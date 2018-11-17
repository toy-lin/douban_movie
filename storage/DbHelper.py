#!/usr/bin/env python3
# coding=utf-8
# author=XingLong Pan
# date=2016-12-06

import pymysql.cursors
import configparser


class DbHelper:

    __connection = None

    def __init__(self):
        self.__connect_database()

    def __connect_database(self):
        config = configparser.ConfigParser()
        config.read('config.ini')

        self.__connection = pymysql.connect(
            host=config['mysql']['host'],
            user=config['mysql']['user'],
            password=config['mysql']['password'],
            db=config['mysql']['db_name'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)
        with self.__connection.cursor() as cursor:
            createDb = "CREATE DATABASE IF NOT EXISTS douban;"
            cursor.execute(createDb)
            self.__connection.commit()
        with self.__connection.cursor() as cursor:    
            createTb = "CREATE TABLE IF NOT EXISTS movie(\
                id INT NOT NULL AUTO_INCREMENT,\
                douban_id VARCHAR(10) UNIQUE,\
                title VARCHAR(100),\
                directors VARCHAR(20),\
                scriptwriters VARCHAR(20),\
                actors VARCHAR(500),\
                types VARCHAR(50),\
                release_region VARCHAR(20),\
                release_date DATE,\
                alias VARCHAR(100) DEFAULT NULL,\
                languages VARCHAR(20),\
                duration VARCHAR(20),\
                score FLOAT,\
                score_count INT,\
                description VARCHAR(2000),\
                tags VARCHAR(100),\
                imdb_id VARCHAR(20),\
                comment_short_count INT,\
                comment_count INT,\
                rate_five_start FLOAT,\
                rate_four_start FLOAT,\
                rate_three_start FLOAT,\
                rate_two_start FLOAT,\
                rate_one_start FLOAT,\
                cover_url VARCHAR(100),\
                PRIMARY KEY (id)\
            )"
            cursor.execute(createTb)
            self.__connection.commit()

    def insert_movie(self, movie):
        try:
            with self.__connection.cursor() as cursor:
                sql = "INSERT IGNORE INTO `movie` (`douban_id`, `title`, `directors`, " \
                      "`scriptwriters`, `actors`, `types`,`release_region`," \
                      "`release_date`,`alias`,`languages`,`duration`,`score`,`score_count`," \
                      "`description`,`tags`,`imdb_id`,`comment_short_count`,`comment_count`,"\
                      "`rate_five_start`,`rate_four_start`,`rate_three_start`,`rate_two_start`,`rate_one_start`,`cover_url`)"\
                      " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
                cursor.execute(sql, (
                    movie['douban_id'],
                    movie['title'],
                    movie['directors'],
                    movie['scriptwriters'],
                    movie['actors'],
                    movie['types'],
                    movie['release_region'],
                    movie['release_date'],
                    movie['alias'],
                    movie['languages'],
                    movie['duration'],
                    movie['score'],
                    movie.get('score_count',0),
                    movie['description'],
                    movie['tags'],
                    movie.get('imdb_id',''),
                    movie['comment_short_count'],
                    movie['comment_count'],
                    movie['rate_five_star'],
                    movie['rate_four_star'],
                    movie['rate_three_star'],
                    movie['rate_two_star'],
                    movie['rate_one_star'],
                    movie['cover_url']
                ))
                self.__connection.commit()
        finally:
            pass

    def is_movie_id_exists(self, id):
        with self.__connection.cursor() as cursor:
            cursor.execute('SELECT COUNT(douban_id) FROM movie WHERE douban_id=%s',id)
            self.__connection.commit()
            return cursor._rows[0]['COUNT(douban_id)']
    
    def get_last_movie_id(self):
        with self.__connection.cursor() as cursor:
            result = cursor.execute('SELECT douban_id FROM movie ORDER BY id DESC limit 1')
            if result <= 0:
                return None
            else:
                return cursor._rows[0]['douban_id']

    def close_db(self):
        self.__connection.close()
