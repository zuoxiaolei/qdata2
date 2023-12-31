import requests
import pytz
from datetime import datetime

tokens = [
    'b0b21688d4694f7999c301386ee90a0c',  # xiaolei
    # '4b4b075475bc41e8a39704008677010f',  # peilin
    # '44b351689de6492ab519a923e4c202da',  # jiayu
]
tz = pytz.timezone('Asia/Shanghai')
now = datetime.now(tz).strftime("%Y-%m-%d")


def send_message(content, token):
    params = {
        'token': token,
        'title': '组合投资策略',
        'content': content,
        'template': 'txt'}
    url = 'http://www.pushplus.plus/send'
    res = requests.get(url, params=params)
    print(res)


def send_ratation_message(last_day, last_day_profit):
    print({"last_day": last_day, "now": now})
    last_day_profit = round(last_day_profit, 2)
    if last_day == now:
        message = f"组合投资\n日期：{last_day}\n涨幅：{last_day_profit}%"
        send_message(message, tokens)


if __name__ == '__main__':
    pass
