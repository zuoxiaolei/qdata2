import os
import re
from concurrent.futures import ThreadPoolExecutor

import akshare as ak
import pandas as pd
import requests
import retrying
import tqdm

thread_num = 10
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"}

@retrying.retry(stop_max_attempt_number=10, stop_max_delay=10000)
def get_fund_scale(code="159819"):
    url = f"https://fund.eastmoney.com/{code}.html"
    resp = requests.get(url, headers=headers)
    resp.encoding = resp.apparent_encoding
    scale = re.findall("基金规模</a>：(.*?)亿元", resp.text)[0].strip()
    return code, float(scale)


def get_fund_scale2(code="159819"):
    try:
        return get_fund_scale(code)
    except:
        None


def get_all_fund_scale():
    fund_etf_fund_daily_em_df = ak.fund_etf_fund_daily_em()
    codes = fund_etf_fund_daily_em_df["基金代码"].tolist()
    with ThreadPoolExecutor(thread_num) as executor:
        fund_scale = list(tqdm.tqdm(executor.map(get_fund_scale2, codes), total=len(codes)))
    fund_scale = [ele for ele in fund_scale if ele]
    fund_scale = pd.DataFrame(fund_scale, columns=['code', 'scale'])
    return fund_scale

