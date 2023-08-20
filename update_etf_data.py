from concurrent.futures import ThreadPoolExecutor
import tqdm
from get_etf_scale import get_all_fund_scale, save_north_flowin
import akshare as ak
import easyquotation
import retrying
from mysql_util import get_connection, time_cost, get_max_date, insert_table_by_batch
from send_message import send_ratation_message
from slope_strategy import load_spark_sql, get_spark, get_ols, get_etf_best_parameter
from pyspark.sql.types import StructType, DoubleType
import time
import pytz

thread_num = 10
tz = pytz.timezone('Asia/Shanghai')


@time_cost
def update_etf_scale():
    scale_df = get_all_fund_scale()
    etf_scale_data = scale_df.values.tolist()
    print("update etf.dim_etf_scale")
    start_time = time.time()
    sql = '''
        replace into etf.dim_etf_scale(code, scale)
        values (%s, %s)
        '''
    insert_table_by_batch(sql, etf_scale_data)
    end_time = time.time()
    print(end_time - start_time)  # 0.12199997901916504


@time_cost
def update_etf_basic_info():
    fund_etf_fund_daily_em_df = ak.fund_etf_fund_daily_em()
    fund_etf_fund_daily_em_df = fund_etf_fund_daily_em_df.sort_values(by=[])
    fund_etf_fund_daily_em_df = fund_etf_fund_daily_em_df[['基金代码', '基金简称', '类型', '折价率']]
    print("update etf.dim_etf_basic_info")
    sql = f'''
        replace into etf.dim_etf_basic_info(code, name, type, discount_rate)
        values (%s, %s, %s, %s)
        '''
    insert_table_by_batch(sql, fund_etf_fund_daily_em_df.values.tolist())


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

    print("update etf.ods_etf_realtime")
    sql = '''
        replace into etf.ods_etf_realtime(code, `date`, open, close, high, low, volume, now, increase_rate)
        values (%s, %s, %s, %s,%s, %s, %s, %s,%s)
        '''
    insert_table_by_batch(sql, realtime_df)


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
            sql = '''
                replace into etf.ods_etf_history(code, date, open, close, high, low, volume)
                values (%s, %s, %s, %s, %s, %s, %s)
                '''
            insert_table_by_batch(sql, df.values.tolist())
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
    res = spark.sql(spark_sql[0])
    update_min_date = get_max_date(n=2)
    res = res.where(res.date >= update_min_date).toPandas()
    res = res.fillna(0)
    print("update etf.dws_etf_slope_history")
    sql = '''
        replace into etf.dws_etf_slope_history(code, `date`, close, slope, slope_mean, slope_std)
        values (%s, %s, %s, %s, %s, %s)
        '''
    insert_table_by_batch(sql, res.values.tolist())


@time_cost
def get_etf_slope_rt():
    max_rt_date = get_max_date(n=1)
    spark_sql = load_spark_sql()

    sql_rt = f'''
    select t1.code, t1.date, (t1.increase_rate+1)*t2.close close
    from (
    select code, date, increase_rate
    from etf.ods_etf_realtime
    where date in (select date from etf.dim_etf_trade_date where rn=1)
    )t1 
    join (
    select code, date, close 
    from etf.ods_etf_history
    where date in (select date from etf.dim_etf_trade_date where rn=2)
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
    print("update etf.dws_etf_slope_realtime")
    print(res.shape)
    sql = '''
        replace into etf.dws_etf_slope_realtime(code, `date`, close, slope)
        values (%s, %s, %s, %s)
        '''
    insert_table_by_batch(sql, res.values.tolist())


def get_buy_sell_history(code):
    sql = '''
    select code, name, date, price close, slope, `signal`
    from etf.ads_etf_strategy_history_rpt
    where code='{}' and `signal` in ('buy', 'sell')
    order by date
    '''.format(code)

    with get_connection() as cursor:
        cursor.execute(sql)
        history_data = cursor.fetchall()
    i, j = 0, 0
    buy_sell_data = []
    while i < len(history_data):
        signal = history_data[i][5]
        if signal == 'buy':
            j = i + 1
            while j < len(history_data):
                signal_sell = history_data[j][5]
                if signal_sell == 'sell':
                    buy_sell_data.append([code, history_data[i][1], history_data[i][2], history_data[j][2],
                                          history_data[j][3] / history_data[i][3] - 1])
                    i = j + 1
                    break
                j += 1
            i = i + 1
        else:
            i += 1
    return buy_sell_data


def get_all_buy_sell_history():
    codes = get_etf_codes()
    for code in tqdm.tqdm(codes):
        buy_sell_data = get_buy_sell_history(code)
        sql = '''
        replace into etf.ads_etf_buy_sell_history
        values (%s, %s, %s, %s, %s)
        '''
        if buy_sell_data:
            insert_table_by_batch(sql, buy_sell_data)


@time_cost
def update_trade_date():
    with get_connection() as cursor:
        sql = '''
        replace into etf.dim_etf_trade_date
        select date,
            row_number() over (order by date desc) rn
        from (
        select date
        from etf.ods_etf_history
        union
        select date
        from etf.ods_etf_realtime
        ) t
        order by date desc
        '''
        cursor.execute(sql)


@time_cost
def update_ratation():
    with get_connection() as cursor:
        sql = '''
        replace into etf.ads_etf_ratation_strategy
        select code, name, start_price, end_price, start_date, end_date, (end_price_lead / start_price - 1) rate
        from (select code,
                     name, date, price, slope, rn1-rn2 rn,
                     first_value(price) over(partition by code, rn1-rn2 order by date) start_price,
                     first_value(price) over(partition by code, rn1-rn2 order by date desc) end_price,
                     first_value(date) over(partition by code, rn1-rn2 order by date) start_date,
                     first_value(date) over(partition by code, rn1-rn2 order by date desc) end_date,
                     first_value(price_lead) over(partition by code, rn1-rn2 order by date desc) end_price_lead
              from (
                  select *,
                  row_number() over(order by date) rn1, row_number() over(partition by code order by date) rn2
                  from (
                  select *,  lag(rn, 1) over(partition by code order by date) last_rn,
                                lag(rn, 2) over(partition by code order by date) last_rn2
                  from (
                    select code, name, date, price, slope, price_lead,
                  ROW_NUMBER() over(partition by date order by slope desc) rn
                  from (
                  select code, name, date, price, price/price_lag-1 slope, price_lead
                  from (
                  select code, name, date, price,
                  lag(price, 20) over(partition by code order by date) price_lag,
                  lead(price, 1) over(partition by code order by date) price_lead
                  from (
                            select code, name, date, price
                            from etf.ads_etf_strategy_history_rpt
                            where code in ('159941', '518880', '159915', '159633', '516970', '159736', '512690', '515700', '159937', '159629', '159928', '512480')
                            and date >='2015-07-13' and date in (select date from etf.dim_etf_trade_date where rn>1)
                            union all
                            select code, name, date, price
                            from etf.ads_etf_strategy_rt_rpt
                            where code in ('159941', '518880', '159915', '159633', '516970', '159736', '512690', '515700', '159937', '159629', '159928', '512480')
                            and date in (select max(date) from etf.dim_etf_trade_date)
                            )t
                  ) t
                  where price_lag is not null
                  ) t
                  )t
                  ) t
                 where rn=1 and last_rn=1 and last_rn2=1
                  ) t
        ) t
        GROUP BY code, name, start_price, end_price, start_date, end_date,  (end_price_lead / start_price - 1)
        order by end_date desc 
        limit 10
        '''
        cursor.execute(sql)


def get_rt_rpt():
    sql = '''
    replace into etf.ads_etf_strategy_rt_rpt(code, name, date, scale, price, slope, last_slope, slope_low, slope_high, `signal`)
    select t1.code,
                t1.name,
                t1.date,
                t1.scale,
                t1.price,
                t1.slope,
                t2.slope last_slope,
                t1.slope_low,
                t1.slope_high,
                case when t1.slope_low is null then NULL
                            when t1.slope>=t1.slope_low and t2.slope<=t1.slope_low then 'buy'
                            when t1.slope>=t1.slope_high then 'sell'
                            else 'hold'
                end as `signal`
                
    from (
    select t1.code,
                t1.date,
                t2.name,
                t3.scale,
                t1.close price,
                (t1.slope-t4.slope_mean)/if(t4.slope_std=0, 1e-6, t4.slope_std)  slope,
                t5.low slope_low,
                t5.high slope_high
    from etf.dws_etf_slope_realtime t1
    join etf.dim_etf_basic_info t2 
    on t1.code=t2.code
    join etf.dim_etf_scale t3 
    on t1.code=t3.code
    join etf.dim_etf_slope_standard_param t4 
    on t1.code=t4.code
    left join etf.dim_etf_slope_best t5
    on t1.code=t5.code
    where t1.date in (select max(date) date from etf.dim_etf_trade_date)
    ) t1
    left join (
    select code, date, (slope-slope_mean)/if(slope_std=0, 1e-6, slope_std)  slope
    from etf.dws_etf_slope_history
    where date in (select date from etf.dim_etf_trade_date where rn=2)
    )t2
    on t1.code=t2.code
    '''
    with get_connection() as cursor:
        cursor.execute(sql)


def get_history_rpt():
    sql = '''
    replace into etf.ads_etf_strategy_history_rpt(code, name, date, scale, price, slope, last_slope, slope_low, slope_high, `signal`)
    select code,
                name,
                date,
                scale,
                price,
                slope,
                last_slope,
                slope_low,
                slope_high,
                case when slope>=slope_low and slope_low>=last_slope then 'buy'
                            when slope>=slope_high then 'sell'
                            else 'hold'
                    end `signal`
    from (
    select t1.code,
                t1.name,
                t1.date,
                t1.scale,
                t1.price,
                t1.slope,
                lag(t1.slope, 1) over(partition by code order by date) last_slope,
                t1.slope_low,
                t1.slope_high,
                null `signal`
    from (
    select t1.code,
                t1.date,
                t2.name,
                t3.scale,
                t1.close price,
                (t1.slope-t4.slope_mean)/if(t4.slope_std=0, 1e-6, t4.slope_std) slope,
                t5.low slope_low,
                t5.high slope_high
    from (select * from etf.dws_etf_slope_history 
                where date in (select date from etf.dim_etf_trade_date where rn<=2)) t1
    join etf.dim_etf_basic_info t2 
    on t1.code=t2.code
    join etf.dim_etf_scale t3 
    on t1.code=t3.code
    join etf.dim_etf_slope_standard_param t4 
    on t1.code=t4.code
    left join etf.dim_etf_slope_best t5
    on t1.code=t5.code
    ) t1
    )t
    where date in (select date from etf.dim_etf_trade_date where rn<=1)
    '''
    with get_connection() as cursor:
        cursor.execute(sql)


def update_mean_std():
    sql = '''
    replace into etf.dim_etf_slope_standard_param(code, slope_mean, slope_std)
    select code, slope_mean, slope_std
    from dws_etf_slope_history
    where date in (select date from etf.dim_etf_trade_date where rn=2)
    '''
    with get_connection() as cursor:
        cursor.execute(sql)


def update_rotation_rank():
    sql = '''
    replace into etf.ads_etf_ratation_rank
	select code, name, date, price, slope						
	from (
						select *
						from (
							select code, name, date, price, slope, price_lead,
						ROW_NUMBER() over(partition by date order by slope desc) rn
						from (
						select code, name, date, price, price/price_lag-1 slope, price_lead
						from (
						select code, name, date, price,
						lag(price, 20) over(partition by code order by date) price_lag,
						lead(price, 1) over(partition by code order by date) price_lead
						from (
											select code, name, date, price
											from etf.ads_etf_strategy_history_rpt
											where code in ('159941', '518880', '159915', '159633', '516970', '159736', '512690', '515700', '159937', '159629', '159928', '512480')
											and date >='2015-07-13' and date in (select date from etf.dim_etf_trade_date where rn>1)
											union all
											select code, name, date, price
											from etf.ads_etf_strategy_rt_rpt
											where code in ('159941', '518880', '159915', '159633', '516970', '159736', '512690', '515700', '159937', '159629', '159928', '512480')
											and date in (select max(date) from etf.dim_etf_trade_date)
											)t
						) t
						where price_lag is not null
						) t
						)t
						) t
					 where rn=1
					 order by date desc
    '''
    with get_connection() as cursor:
        cursor.execute(sql)


def run_every_minute():
    update_etf_realtime()
    update_trade_date()
    get_etf_slope_rt()
    get_rt_rpt()
    update_ratation()
    send_ratation_message()
    update_rotation_rank()


def run_every_day():
    update_etf_scale()
    update_etf_basic_info()
    update_etf_history_data()
    get_etf_slope()
    update_mean_std()
    get_etf_best_parameter()
    get_all_buy_sell_history()
    get_history_rpt()
    save_north_flowin()


if __name__ == "__main__":
    run_every_minute()
    # run_every_minute()

    # update_etf_scale()

    # max_rt_date = get_max_date()
    # print(max_rt_date)
