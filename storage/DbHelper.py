#!/usr/bin/env python3
# coding=utf-8
# author=XingLong Pan
# date=2016-12-06

import configparser

import pymysql.cursors


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
                      "`description`,`tags`,`imdb_id`,`comment_short_count`,`comment_count`," \
                      "`rate_five_start`,`rate_four_start`,`rate_three_start`,`rate_two_start`,`rate_one_start`,`cover_url`)" \
                      " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
                cursor.execute(sql, (
                    movie['douban_id'],
                    movie.get('title', ''),
                    movie.get('directors', ''),
                    movie.get('scriptwriters', ''),
                    movie.get('actors', ''),
                    movie.get('types', ''),
                    movie.get('release_region', ''),
                    movie.get('release_date', ''),
                    movie.get('alias', ''),
                    movie.get('languages', ''),
                    movie.get('duration', ''),
                    movie.get('score', 0),
                    movie.get('score_count', 0),
                    movie.get('description', ''),
                    movie.get('tags', ''),
                    movie.get('imdb_id', ''),
                    movie.get('comment_short_count', 0),
                    movie.get('comment_count', 0),
                    movie.get('rate_five_star', 0),
                    movie.get('rate_four_star', 0),
                    movie.get('rate_three_star', 0),
                    movie.get('rate_two_star', 0),
                    movie.get('rate_one_star', 0),
                    movie.get('cover_url', '')
                ))
                self.__connection.commit()
        finally:
            pass

    def is_movie_id_exists(self, id):
        with self.__connection.cursor() as cursor:
            cursor.execute('SELECT COUNT(douban_id) FROM movie WHERE douban_id=%s', id)
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
