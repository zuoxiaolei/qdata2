import random
import sys
from datetime import datetime
import numpy as np
import psutil
import pytz
import pandas as pd
from pyspark.sql import SparkSession, udf
from pyspark.sql.types import *
from StockData import get_etf_strategy, run_every_day

cpu_count = psutil.cpu_count()
windows = 110
qdata_prefix = "https://raw.githubusercontent.com/zuoxiaolei/qdata/main/data/"
github_proxy_prefix = "https://ghproxy.com/"
sequence_length = 10
seq_length = 30
tz = pytz.timezone('Asia/Shanghai')
now = datetime.now(tz).strftime("%Y%m%d")


def load_spark_sql():
    sql_text = open('data/spark.sql', encoding='utf-8').read()
    spark_sql = [ele for ele in sql_text.split(";") if ele]
    return spark_sql


spark_sql = load_spark_sql()


def get_spark():
    parallel_num = str(cpu_count * 3)
    spark = SparkSession.builder \
        .appName("chain merge") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", parallel_num) \
        .config("spark.default.parallelism", parallel_num) \
        .config("spark.ui.showConsoleProgress", True) \
        .config("spark.executor.memory", '1g') \
        .config("spark.driver.memory", '2g') \
        .config("spark.driver.maxResultSize", '2g') \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.executor.extraJavaOptions", "-Xss1024M") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


spark = get_spark()


def get_ols(x, y):
    '''线性回归'''
    if not x or not y:
        return float(np.NAN), float(np.NAN), float(np.NAN)
    if len(x) != len(y):
        return None, None, None
    x = np.array(x)
    y = np.array(y)
    slope, intercept = np.polyfit(x, y, 1)
    r2 = (sum((y - (slope * x + intercept)) ** 2) / ((len(y) - 1) * np.var(y, ddof=1)))
    return float(slope), float(intercept), float(r2)


schema = StructType().add("slope", DoubleType()).add("intercept", DoubleType()).add("r2", DoubleType())
spark.udf.register('get_ols', get_ols, schema)


def get_etf_slope():
    etf_df_filename = "data/ads/exchang_fund_rt.csv"
    etf_df = pd.read_csv(etf_df_filename, dtype={"code": object})
    etf_df = spark.createDataFrame(etf_df)
    dfs = [etf_df]
    res = []
    for ele in dfs:
        ele.createOrReplaceTempView("df")
        res.append(spark.sql(spark_sql[0]).toPandas())
    res[0].to_csv('data/rsrs_etf.csv', index=False)
    res[0].groupby('code').tail(20).to_csv('data/rsrs_etf_latest.csv', index=False)


def get_stock_slope():
    stock_df = spark.read.csv("data/ods/market_df", header=True, inferSchema=True)
    dfs = [stock_df]
    res = []
    for ele in dfs:
        ele.createOrReplaceTempView("df")
        res.append(spark.sql(spark_sql[0]).toPandas())
    res[0].groupby('code').tail(20).to_csv('data/stock_rsrs_latest.csv', index=False)


def get_fund_slope():
    fund_df = spark.read.csv("data/ods/fund", header=True, inferSchema=True)
    dfs = [fund_df]
    res = []
    for ele in dfs:
        ele.createOrReplaceTempView("df")
        res.append(spark.sql(spark_sql[0]).toPandas())
    res[0].groupby('code').tail(20).to_csv('data/rsrs_fund_latest.csv', index=False)


def tune_best_param(df, low=-0.5, high=1.5):
    if low >= high:
        return 0
    length = len(df)
    i = 0
    code = df['code'].unique().tolist()[0]
    name = df['name'].unique().tolist()[0]
    slope_standard = df['slope_standard'].tolist()
    close = df['close'].tolist()
    date = df['date'].tolist()
    results = []
    previous_rsrs = None
    current_rsrs = None
    while i < length:
        previous_rsrs = current_rsrs
        current_rsrs = slope_standard[i]
        current_close = close[i]
        current_date = date[i]
        if previous_rsrs and current_rsrs >= low > previous_rsrs:
            j = i
            while j < length:
                next_rsrs = slope_standard[j]
                next_close = close[j]
                next_date = date[j]
                if next_rsrs >= high:
                    results.append([code, name, current_date,
                                    next_date, current_close, next_close, next_close / current_close - 1])
                    i = j
                    break
                j += 1
        i += 1
    return results


from itertools import product


def find_best_param():
    dfs = pd.read_csv('temp.csv', dtype={'code': object})
    dfs = dfs[dfs.date >= '2013-01-01']
    codes = dfs[['code', 'name']].drop_duplicates().values.tolist()
    all_result = []
    for code, name in codes:
        df = dfs[dfs.code == code]
        lows = [-i * 0.1 for i in range(11)]
        highs = [0.1 * i for i in range(1, 20)]
        find_result = []
        for low, high in product(lows, highs):
            results = tune_best_param(df, low=low, high=high)
            df1 = pd.DataFrame(results, columns=['code', 'name', 'current_date', 'next_date',
                                                 'current_close', 'next_close', 'profit'])
            find_result.append([low, high, df1['profit'].sum()])
        find_result = sorted(find_result, key=lambda x: x[-1], reverse=True)
        all_result.append([code, name] + find_result[0])
    all_result = pd.DataFrame(all_result, columns=['code', 'name', 'low', 'high', 'profit'])
    all_result.to_csv('temp4.csv', index=False)


def merge_rsrs():
    etf_info = pd.read_csv("data/dim/exchang_eft_basic_info.csv", dtype={'基金代码': object})[['基金代码', '基金简称']]
    etf_info.columns = ['code', 'name']
    stock_info = pd.read_csv("data/dim/stock_zh_a_spot_em_df.csv", dtype={'code': object})[['code', 'name']]
    df2 = pd.read_csv('data/stock_rsrs_latest.csv', dtype={'code': object})
    df3 = pd.read_csv('data/rsrs_etf_latest.csv', dtype={'code': object})
    df2['code'] = df2['code'].map(lambda x: ''.join(['0'] * (6 - len(x))) + x)
    df3['code'] = df3['code'].map(lambda x: ''.join(['0'] * (6 - len(x))) + x)
    df2 = df2.merge(stock_info, on='code', how='left')
    df3 = df3.merge(etf_info, on='code', how='left')
    df = pd.concat([df2, df3], axis=0)

    df.to_csv('data/rsrs.csv', index=False)
    fund_info = pd.read_csv("data/dim/fund_name_em_df.csv", dtype={'code': object})[['code', 'name']]
    df1 = pd.read_csv('data/rsrs_fund_latest.csv', dtype={'code': object})
    df1['code'] = df1['code'].map(lambda x: ''.join(['0'] * (6 - len(x))) + x)
    df1 = df1.merge(fund_info, on='code', how='left')
    df1.to_csv('data/rsrs_fund.csv', index=False)


def rsrs_strategy(dates, df_dict, low=-0.2, high=0.9):
    length = len(dates)
    buy_states = False
    current_code = None
    money = 1
    buy_price = None
    log_data = []
    buy_date = None

    for i in range(length):
        sub_df = df_dict[dates[i]]
        if not buy_states:
            slope_array = sub_df['slope_standard'].values
            find_index = np.argmin(np.abs(slope_array - low))
            rsrs = sub_df.iloc[find_index]['slope_standard']
            if abs(rsrs - low) <= 0.1:
                buy_states = True
                buy_price = sub_df.iloc[find_index]['close']
                current_code = sub_df.iloc[find_index]['code']
                buy_date = sub_df.iloc[find_index]['date']
        else:
            sub_df = sub_df[sub_df.code == current_code]
            if not sub_df.empty:
                slope_array = sub_df['slope_standard'].values
                find_index = np.argmin(np.abs(slope_array - high))
                rsrs = sub_df.iloc[find_index]['slope_standard']
                if abs(rsrs - high) <= 0.1:
                    buy_states = False
                    sell_price = sub_df.iloc[find_index]['close']
                    money = money * (sell_price / buy_price)
                    log_data.append([current_code, buy_date, dates[i], sell_price / buy_price])
    return money, log_data


def run():
    args = sys.argv
    if len(args) > 1:
        if args[1] == "get_etf_slope":
            run_every_day()
            get_etf_slope()
            merge_rsrs()
            get_etf_strategy()
        elif args[1] == "get_stock_slope":
            get_stock_slope()
            merge_rsrs()
            get_etf_strategy()
        elif args[1] == "get_fund_slope":
            get_fund_slope()
            merge_rsrs()
            get_etf_strategy()
        else:
            print("qtrade3 usage: python qtrade3.py get_etf_slope|get_stock_slope|get_fund_slope")


if __name__ == '__main__':
    run()
