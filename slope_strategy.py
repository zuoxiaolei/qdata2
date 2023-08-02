import numpy as np
import pandas as pd
import psutil
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, DoubleType
from tqdm import tqdm
from mysql_util import get_connection, time_cost

cpu_count = psutil.cpu_count()


def get_profit(df, low=-0.2, high=0.8):
    i, j = 1, 1
    rsrs_list = df['slope_standard'].tolist()
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
                    result.append([dates[i], dates[j], close_list[j] / close_list[i] - 1])
                    break
                j += 1
            i = j + 1
        else:
            i = i + 1
    result = pd.DataFrame(result, columns=['date_buy', 'date_sell', 'profit'])
    return result


def get_best_parameter(df):
    lows = [i / 10 - 2 for i in range(41)]
    highs = [i / 10 - 2 for i in range(41)]
    from itertools import product
    result = []
    for low, high in product(lows, highs):
        res = get_profit(df, low=low, high=high)
        profit = res['profit'].sum()
        result.append([low, high, profit])
    result.sort(key=lambda x: x[2], reverse=True)
    return result[0]


def get_all_best_parameter():
    df_rsrs = pd.read_csv("data/rsrs_etf.csv", dtype={'code': object})
    dfs = dict(list(df_rsrs.groupby('code', as_index=False)))
    best_params = []
    buy_dfs = []
    for code, df in tqdm(list(dfs.items())):
        low, high, _ = get_best_parameter(df)
        best_params.append([code, low, high])
        buy_df = get_profit(df, low=low, high=high)
        buy_df["code"] = code
        buy_dfs.append(buy_df)
    pd.DataFrame(best_params, columns=['code', 'low', 'high']).to_csv("best_params.csv", index=False)
    buy_dfs = pd.concat(buy_dfs, axis=0).sort_values("date_buy")
    buy_dfs.to_csv("buy_dfs.csv", index=False)


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


@time_cost
def get_etf_slope():
    spark_sql = load_spark_sql()
    spark = get_spark()
    schema = StructType().add("slope", DoubleType()).add("intercept", DoubleType()).add("r2", DoubleType())
    spark.udf.register('get_ols', get_ols, schema)
    with get_connection() as cursor:
        sql = '''
        select code, date, close
        from etf.ods_etf_history
        '''
        cursor.execute(sql)
        etf_df = cursor.fetchall()

    etf_df = spark.createDataFrame(etf_df, ['code', 'date', 'close'])
    etf_df.createOrReplaceTempView("df")
    res = spark.sql(spark_sql[0]).toPandas()
    res = res.fillna(0)
    with get_connection() as cursor:
        sql = '''
        replace into etf.dws_etf_slope_history(code, `date`, close, slope, slope_mean, slope_std)
        values (%s, %s, %s, %s, %s, %s)
        '''
        cursor.executemany(sql, res.values.tolist())


def get_etf_slope_rt():
    pass


if __name__ == '__main__':
    import time

    start_time = time.time()
    get_etf_slope()
    end_time = time.time()
    print(end_time - start_time)
