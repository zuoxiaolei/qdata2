from concurrent.futures import ThreadPoolExecutor
import tqdm
from get_etf_scale import get_all_fund_scale
import akshare as ak
import easyquotation
import retrying
from mysql_util import get_connection, time_cost, get_max_date
from slope_strategy import load_spark_sql, get_spark, get_ols
from pyspark.sql.types import StructType, DoubleType
import time
import pytz

thread_num = 10
tz = pytz.timezone('Asia/Shanghai')

@time_cost
def update_etf_scale():
    scale_df = get_all_fund_scale()
    etf_scale_data = scale_df.values.tolist()
    with get_connection() as cursor:
        sql = f'''
        replace into etf.dim_etf_scale(code, scale)
        values (%s, %s)
        '''
        cursor.executemany(sql, etf_scale_data)


@time_cost
def update_etf_basic_info():
    fund_etf_fund_daily_em_df = ak.fund_etf_fund_daily_em()
    fund_etf_fund_daily_em_df = fund_etf_fund_daily_em_df.sort_values(by=[])
    fund_etf_fund_daily_em_df = fund_etf_fund_daily_em_df[['基金代码', '基金简称', '类型', '折价率']]
    with get_connection() as cursor:
        sql = f'''
        replace into etf.dim_etf_basic_info(code, name, type, discount_rate)
        values (%s, %s, %s, %s)
        '''
        cursor.executemany(sql, fund_etf_fund_daily_em_df.values.tolist())


@time_cost
def get_etf_codes():
    with get_connection() as cursor:
        sql = '''select distinct code from etf.dim_etf_basic_info'''
        cursor.execute(sql)
        res = cursor.fetchall()
        codes = [ele[0] for ele in res]
    return codes



@time_cost
def update_etf_realtime():
    codes = get_etf_codes()

    with get_connection() as cursor:
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
            increase_rate = (now / close) - 1
            realtime_df.append([code, date, open, close, high, low, volume, now, increase_rate])
        sql = '''
        replace into etf.ods_etf_realtime(code, `date`, open, close, high, low, volume, now, increase_rate)
        values (%s, %s, %s, %s,%s, %s, %s, %s,%s)
        '''
        cursor.executemany(sql, realtime_df)


@time_cost
def update_etf_history_data(full=False):
    codes = get_etf_codes()
    start_date = get_max_date(n=1)
    start_date = start_date.replace('-', '')
    start_date = "19900101" if full else start_date

    @retrying.retry(stop_max_attempt_number=5)
    def get_exchange_fund_data(code):
        try:
            df = ak.fund_etf_hist_em(symbol=code, period="daily", start_date=start_date,
                                     end_date="21000101", adjust="hfq")
            columns = ['日期', '开盘', '收盘', '最高', '最低', '成交量']
            df = df[columns]
            df.columns = ['date', 'open', 'close', 'high', 'low', 'volume']
            df['code'] = code
            df = df[['code', 'date', 'open', 'close', 'high', 'low', 'volume']]
            with get_connection() as cursor:
                sql = '''
                replace into etf.ods_etf_history(code, date, open, close, high, low, volume)
                values (%s, %s, %s, %s, %s, %s, %s)
                '''
                cursor.executemany(sql, df.values.tolist())
            time.sleep(0.5)
            del df
        except Exception as e:
            import traceback
            traceback.print_exc()

    with ThreadPoolExecutor(thread_num) as executor:
        list(tqdm.tqdm(executor.map(get_exchange_fund_data, codes), total=len(codes)))


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

@time_cost
def get_etf_slope_rt():
    max_rt_date = get_max_date()
    spark_sql = load_spark_sql()

    sql_rt = f'''
    select t1.code, t1.date, (t1.increase_rate+1)*t2.close close
    from (
    select code, date, increase_rate
    from etf.ods_etf_realtime
    where date in (select max(date) from etf.ods_etf_realtime)
    )t1 
    join (
    select code, date, close 
    from etf.ods_etf_history
    where date='{max_rt_date}'
    )t2
    on t1.code=t2.code
    union all
    select code, date, close
    from etf.ods_etf_history
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
    with get_connection() as cursor:
        sql = '''
        replace into etf.dws_etf_slope_realtime(code, `date`, close, slope)
        values (%s, %s, %s, %s)
        '''
        cursor.executemany(sql, res.values.tolist())


def run_every_minute():
    update_etf_realtime()
    get_etf_slope_rt()



def run_every_day():
    update_etf_scale()
    update_etf_basic_info()
    update_etf_history_data()
    get_etf_slope()


if __name__ == "__main__":
    # run_every_day()
    run_every_minute()
