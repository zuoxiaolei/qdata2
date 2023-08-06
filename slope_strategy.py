import numpy as np
import pandas as pd
import psutil
from pyspark.sql import SparkSession
from tqdm import tqdm
from mysql_util import time_cost, get_connection
from concurrent.futures import ProcessPoolExecutor

cpu_count = psutil.cpu_count()


def get_profit(df, low=-0.2, high=0.8):
    i, j = 1, 1
    rsrs_list = df['slope'].tolist()
    dates = df['date'].tolist()
    close_list = df['close'].tolist()
    result = []
    while i < len(df):
        index_value = rsrs_list[i]
        index_value_last = rsrs_list[i - 1]
        if index_value_last <= low <= index_value:
            j = i + 1
            while j < len(df):
                sell_index_value = rsrs_list[j]
                if sell_index_value >= high:
                    result.append([dates[i], dates[j], close_list[j] / (close_list[i] + 1e-6) - 1])
                    break
                j += 1
            i = j + 1
        else:
            i = i + 1
    result = pd.DataFrame(result, columns=['date_buy', 'date_sell', 'profit'])
    return result['profit'].sum()


def get_best_parameter(df):
    lows = [i / 10 - 3 for i in range(61)]
    highs = [i / 10 - 3 for i in range(61)]
    from itertools import product
    result = []
    for low, high in product(lows, highs):
        if low < high and high - low >= 0.5:
            profit = get_profit(df, low=low, high=high)
            result.append([low, high, profit])
    result.sort(key=lambda x: x[2], reverse=True)
    return result[0]


def find_rsrs_task(args):
    code, df = args
    low, high, _ = get_best_parameter(df)
    return (code, low, high)


@time_cost
def get_all_best_parameter(df_rsrs):
    dfs = dict(list(df_rsrs.groupby('code', as_index=False)))
    with ProcessPoolExecutor() as executor:
        best_params = list(tqdm(executor.map(find_rsrs_task, list(dfs.items())), total=len(dfs)))
    best_params = pd.DataFrame(best_params, columns=['code', 'low', 'high'])
    return best_params


@time_cost
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


@time_cost
def load_spark_sql():
    sql_text = open('data/spark.sql', encoding='utf-8').read()
    spark_sql = [ele for ele in sql_text.split(";") if ele]
    return spark_sql


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


def get_etf_best_parameter():
    with get_connection() as cursor:
        sql = '''
        select code,
               date,
               close,
               (slope-slope_mean)/if(slope_std=0, 1e-6, slope_std) slope
        from etf.dws_etf_slope_history
        '''
        cursor.execute(sql)
        df = cursor.fetchall()
    df = pd.DataFrame(df, columns=['code', 'date', 'close', 'slope'])
    best_params = get_all_best_parameter(df)
    with get_connection() as cursor:
        sql = '''
        replace into etf.dim_etf_slope_best(code, low, high)
        values (%s, %s, %s)
        '''
        cursor.executemany(sql, best_params.values.tolist())


def get_stock_best_parameter():
    with get_connection() as cursor:
        sql = '''
        select code,
               date,
               close,
               (slope-slope_mean)/if(slope_std=0, 1e-6, slope_std) slope
        from stock.dws_stock_slope_history
        '''
        cursor.execute(sql)
        df = cursor.fetchall()
    df = pd.DataFrame(df, columns=['code', 'date', 'close', 'slope'])
    best_params = get_all_best_parameter(df)
    with get_connection() as cursor:
        sql = '''
        replace into stock.dim_stock_slope_best(code, low, high)
        values (%s, %s, %s)
        '''
        cursor.executemany(sql, best_params.values.tolist())


if __name__ == '__main__':
    get_stock_best_parameter()
