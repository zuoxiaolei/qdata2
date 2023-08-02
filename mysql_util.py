import time

import pymysql
import os
from contextlib import contextmanager

host = os.environ['MYSQL_IP']
port = os.environ['MYSQL_PORT']
user = os.environ['MYSQL_USER']
password = os.environ['MYSQL_PASSWORD']
database = 'etf'
thread_num = 10


def get_mysql_connection(database):
    conn = pymysql.connect(
        host=host,  # 主机名
        port=int(port),  # 端口号，MySQL默认为3306
        user=user,  # 用户名
        password=password,  # 密码
        database=database,  # 数据库名称
    )
    return conn


@contextmanager
def get_connection():
    conn = get_mysql_connection(database)
    cursor = conn.cursor()
    yield cursor
    conn.commit()
    cursor.close()
    conn.close()


def time_cost(func):
    def deco(*args, **kwargs):
        start_time = time.time()
        res = func(*args, **kwargs)
        end_time = time.time()
        print(f"func.__name__ use {end_time - start_time}second")
        return res

    return deco
