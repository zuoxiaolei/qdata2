from concurrent.futures import ThreadPoolExecutor
import tqdm
import akshare as ak
import easyquotation
import retrying
from mysql_util import get_connection, time_cost, get_max_date, insert_table_by_batch
from slope_strategy import load_spark_sql, get_spark, get_ols, get_stock_best_parameter
from pyspark.sql.types import StructType, DoubleType
import time
import pytz

thread_num = 10
tz = pytz.timezone('Asia/Shanghai')


@time_cost
def update_stock_basic_info():
    stocks = ak.stock_zh_a_spot_em()
    stocks = stocks[['代码', '名称', '总市值', '流通市值']]
    stocks = stocks.fillna(0)
    sql = f'''
        replace into stock.dim_stock_basic_info(code, name, scale, scale_market)
        values (%s, %s, %s, %s)
        '''
    insert_table_by_batch(sql, stocks.values.tolist())


@time_cost
def get_stock_codes():
    with get_connection() as cursor:
        sql = '''select distinct code from stock.dim_stock_basic_info'''
        cursor.execute(sql)
        res = cursor.fetchall()
        codes = [ele[0] for ele in res]
    return codes


@time_cost
def update_stock_realtime():
    codes = get_stock_codes()
    quotation = easyquotation.use('sina')
    realtime_data = quotation.stocks(codes)
    realtime_df = []
    for code in realtime_data:
        real_stock = realtime_data[code]
        date = real_stock['date']
        open = real_stock["open"]
        close = real_stock['close']
        high = real_stock['high']
        low = real_stock['low']
        volume = real_stock['volume']
        now = real_stock['now']
        increase_rate = (now / (close + 1e-6)) - 1
        realtime_df.append([code, date, open, close, high, low, volume, now, increase_rate])
    sql = '''
    replace into stock.ods_stock_realtime(code, `date`, open, close, high, low, volume, now, increase_rate)
    values (%s, %s, %s, %s,%s, %s, %s, %s,%s)
    '''
    insert_table_by_batch(sql, realtime_df)


@time_cost
def update_stock_history_data(full=False):
    codes = get_stock_codes()
    start_date = get_max_date(n=1)
    start_date = start_date.replace('-', '')
    start_date = "19900101" if full else start_date

    @retrying.retry(stop_max_attempt_number=5)
    def get_exchange_stock_data(code):
        try:
            stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="hfq", start_date=start_date)
            if len(stock_zh_a_hist_df) > 0:
                stock_zh_a_hist_df.columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'turnover', 'increase',
                                              'increase_rate', 'increase_amount', 'exchange_rate']
                stock_zh_a_hist_df['code'] = code
                df = stock_zh_a_hist_df[['code', 'date', 'open', 'close', 'high', 'low', 'volume']]
                sql = '''
                replace into stock.ods_stock_history(code, date, open, close, high, low, volume)
                values (%s, %s, %s, %s, %s, %s, %s)
                '''
                insert_table_by_batch(sql, df.values.tolist())
                time.sleep(0.5)
                del df
        except Exception as e:
            import traceback
            traceback.print_exc()

    with ThreadPoolExecutor(thread_num) as executor:
        list(tqdm.tqdm(executor.map(get_exchange_stock_data, codes), total=len(codes)))


@time_cost
def get_stock_slope():
    spark_sql = load_spark_sql()
    spark = get_spark()
    schema = StructType().add("slope", DoubleType()).add("intercept", DoubleType()).add("r2", DoubleType())
    spark.udf.register('get_ols', get_ols, schema)
    with get_connection() as cursor:
        sql = '''
        select code, date, close
        from stock.ods_stock_history
        where date in (select date from etf.dim_etf_trade_date where rn<=600)
        '''
        cursor.execute(sql)
        etf_df = cursor.fetchall()

    etf_df = spark.createDataFrame(etf_df, ['code', 'date', 'close'])
    etf_df.createOrReplaceTempView("df")
    res = spark.sql(spark_sql[0])
    update_min_date = get_max_date(n=5)
    res = res.where(res.date >= update_min_date).toPandas()
    res = res.fillna(0)
    sql = '''
    replace into stock.dws_stock_slope_history(code, `date`, close, slope, slope_mean, slope_std)
    values (%s, %s, %s, %s, %s, %s)
    '''
    insert_table_by_batch(sql, res.values.tolist())


@time_cost
def get_stock_slope_rt():
    max_rt_date = get_max_date()
    spark_sql = load_spark_sql()

    sql_rt = f'''
    select t1.code, t1.date, (t1.increase_rate+1)*t2.close close
    from (
    select code, date, increase_rate
    from stock.ods_stock_realtime
    where date in (select date from etf.dim_etf_trade_date where rn=1)
    )t1 
    join (
    select code, date, close 
    from stock.ods_stock_history
    where date in (select date from etf.dim_etf_trade_date where rn=2)
    )t2
    on t1.code=t2.code
    union all
    select code, date, close
    from stock.ods_stock_history
    where date in (select date from etf.dim_etf_trade_date where rn<=20) and date<'{max_rt_date}'
    '''
    with get_connection() as cursor:
        cursor.execute(sql_rt)
        etf_df = cursor.fetchall()
    spark = get_spark()
    schema = StructType().add("slope", DoubleType()).add("intercept", DoubleType()).add("r2", DoubleType())
    spark.udf.register('get_ols', get_ols, schema)
    etf_df = spark.createDataFrame(etf_df, ['code', 'date', 'close'])
    etf_df.createOrReplaceTempView("df")
    res = spark.sql(spark_sql[1].format(max_rt_date)).toPandas()
    res = res.fillna(0)
    sql = '''
    replace into stock.dws_stock_slope_realtime(code, `date`, close, slope)
    values (%s, %s, %s, %s)
    '''
    insert_table_by_batch(sql, res.values.tolist())


def run_every_minute():
    update_stock_realtime()
    get_stock_slope_rt()


def run_every_day():
    update_stock_basic_info()
    update_stock_history_data()
    get_stock_slope()
    get_stock_best_parameter()


if __name__ == "__main__":
    # run_every_minute()
    run_every_day()
