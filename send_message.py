import pandas as pd
import requests
import pytz
from datetime import datetime
from mysql_util import get_connection

tokens = [
    'b0b21688d4694f7999c301386ee90a0c',  # xiaolei
    # '4b4b075475bc41e8a39704008677010f',  # peilin
    # '44b351689de6492ab519a923e4c202da',  # jiayu
]
tz = pytz.timezone('Asia/Shanghai')
now = datetime.now(tz).strftime("%Y-%m-%d")
now = '2023-08-04'


def send_message(content, token):
    params = {
        'token': token,
        'title': now + '轮动策略',
        'content': content,
        'template': 'txt'}
    url = 'http://www.pushplus.plus/send'
    res = requests.get(url, params=params)
    print(res)


def send_ratation_message():
    sql = '''
    select code, name, start_date, end_date
    from  etf.ads_etf_ratation_strategy
    order by end_date desc
    '''
    with get_connection() as cursor:
        cursor.execute(sql)
        data = cursor.fetchall()
    code, name, start_date, end_date = data[0]
    isin_table = False
    with get_connection() as cursor:
        sql = f"""
        select *
        from etf.ads_etf_ratation_push
        where code=%s and start_date=%s and end_date=%s
        """
        cursor.execute(sql, (code, start_date, end_date))
        data = cursor.fetchall()
        if data:
            isin_table = True

    if not isin_table:
        message = f"{code}\t{name}\t{start_date}\t{end_date}"
        send_message(message, tokens)
        with get_connection() as cursor:
            sql = '''
            replace into etf.ads_etf_ratation_push
            values (%s, %s, %s)
            '''
            cursor.execute(sql, (code, start_date, end_date))


if __name__ == '__main__':
    from pprint import pprint

    send_ratation_message()
