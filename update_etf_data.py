from concurrent.futures import ThreadPoolExecutor
import tqdm
from get_etf_scale import get_all_fund_scale
import akshare as ak
import retrying
from mysql_util import get_connection, time_cost, get_max_date, insert_table_by_batch
from send_message import send_ratation_message
from get_spark import get_spark
import time
import pytz
import pandas as pd
import numpy as np
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
def update_trade_date():
    with get_connection() as cursor:
        sql = '''
        replace into etf.dim_etf_trade_date
        select date,
            row_number() over (order by date desc) rn
        from (
        select date
        from etf.ods_etf_history
        ) t
        order by date desc
        '''
        cursor.execute(sql)


@time_cost
def update_ratation():
    with get_connection() as cursor:
        sql = '''
        replace into etf.ads_etf_ratation_strategy
        SELECT code, name, start_price, end_price, start_date
            , end_date, end_price_lead / start_price - 1 AS rate
        FROM (
            SELECT code, name, date, price, slope
                , rn1 - rn2 rn_sub
                , first_value(price) OVER (PARTITION BY code, rn1 - rn2 ORDER BY date) AS start_price
                , first_value(price) OVER (PARTITION BY code, rn1 - rn2 ORDER BY date DESC) AS end_price
                , first_value(date) OVER (PARTITION BY code, rn1 - rn2 ORDER BY date) AS start_date
                , first_value(date) OVER (PARTITION BY code, rn1 - rn2 ORDER BY date DESC) AS end_date
                , first_value(price_lead) OVER (PARTITION BY code, rn1 - rn2 ORDER BY date DESC) AS end_price_lead
                , rn 
                , last_rn
            FROM (
                SELECT *, row_number() OVER (PARTITION BY code, rn ORDER BY date) AS rn2,
                        row_number() OVER (ORDER BY date) AS rn1
                FROM (
                    SELECT *, lag(rn, 1) OVER (PARTITION BY code ORDER BY date) AS last_rn
                        , lag(rn, 2) OVER (PARTITION BY code ORDER BY date) AS last_rn2
                    FROM (
                        SELECT code, name, date, price, slope, date_lead
                            , price_lead, ROW_NUMBER() OVER (PARTITION BY date ORDER BY slope DESC) AS rn
                        FROM (
                            SELECT code, name, date, price
                                , price / price_lag - 1 AS slope, price_lead, date_lead
                            FROM (
                                SELECT code, name, date, price
                                    , lag(price, 20) OVER (PARTITION BY code ORDER BY date) AS price_lag
                                    , lead(price, 1) OVER (PARTITION BY code ORDER BY date) AS price_lead
                                    , lead(date, 1) OVER (PARTITION BY code ORDER BY date) AS date_lead
                                    
                                FROM (
                                    SELECT t1.code, t2.name, date, close price
                                    FROM etf.ods_etf_history t1
                                    join etf.dim_etf_basic_info t2
                                      on t1.code=t2.code
                                    WHERE t1.code IN ('517180', '159637', '515880', '159636', '159941', '513300', '515000', '512480', '588200', '159790', '159995', '515080', '159997', '159870', '512710', '159639', '518880', '512890', '159869', '159875', '159632', '513120', '512500', '159994', '159647', '159939', '159801', '560090', '159820', '159601', '516160', '159806', '159922', '159516', '159845')
                                        AND t1.date >= '2015-07-13'
                                        AND t1.date IN (
                                            SELECT date
                                            FROM etf.dim_etf_trade_date
                                            WHERE rn > 1
                                        )
                                ) t
                            ) t
                            WHERE price_lag IS NOT NULL
                        ) t
                    ) t
                ) t
                WHERE rn = 1
            ) t
        where last_rn = 1
        ) t
        GROUP BY code, name, start_price, end_price, start_date, end_date, end_price_lead / start_price - 1
        ORDER BY end_date DESC
	limit 10
        '''
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
									SELECT t1.code, t2.name, date, close price
                                    FROM etf.ods_etf_history t1
                                    join etf.dim_etf_basic_info t2
                                      on t1.code=t2.code
                                    WHERE t1.code IN ('517180', '159637', '515880', '159636', '159941', '513300', '515000', '512480', '588200', '159790', '159995', '515080', '159997', '159870', '512710', '159639', '518880', '512890', '159869', '159875', '159632', '513120', '512500', '159994', '159647', '159939', '159801', '560090', '159820', '159601', '516160', '159806', '159922', '159516', '159845')
                                        AND t1.date >= '2015-07-13'
                                        AND t1.date IN (
                                            SELECT date
                                            FROM etf.dim_etf_trade_date
                                            WHERE rn > 1
                                        )
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

def get_portfolio_report():
    sql = '''
    select code, date, close, (close-close_lag1)/close_lag1*100 rate
    from (
    select code, date, close, lag(close, 1) over (partition by code order by date) close_lag1
    from (
    select *, count(1) over (partition by date) cnt
    from etf.ods_etf_history
    where code in ('518880', '512890', '159941')
    ) t 
    where cnt=3 and date>='2019-01-18'
    )t
    where close_lag1 is not null
    order by date
    '''
    with get_connection() as cursor:
        cursor.execute(sql)
        data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['code', 'date', 'close', 'rate'])
    spark = get_spark()
    spark_df = spark.createDataFrame(df)
    spark_df = spark_df.groupby("date").pivot("code").mean("rate")
    pandas_df = spark_df.toPandas()
    pandas_df = pandas_df.sort_values(by='date')
    pandas_df.index = pandas_df['date']
    pandas_df = pandas_df.drop(['date'], axis=1)
    weight_dict = {'518880': 0.4997765583588797,
                   '512890': 0.29836978509792955,
                   '159941': 0.20185365654319073}
    columns = ['518880', '512890', '159941']
    weight_list = [weight_dict[k] for k in columns]
    pandas_df = pandas_df[columns]
    pandas_df['rate'] = pandas_df.apply(lambda x: np.dot(weight_list, x.tolist()), axis=1)
    pandas_df['rate_cum'] = pandas_df['rate'] / 100 + 1
    pandas_df['rate_cum'] = pandas_df['rate_cum'].cumprod()
    data = list(zip(pandas_df.index.tolist(), pandas_df["rate_cum"].tolist()))
    sql = '''
    replace into etf.ads_eft_portfolio_rpt
    values (%s, %s)
    '''
    insert_table_by_batch(sql, data)


def run_every_day():
    update_etf_scale()
    update_etf_basic_info()
    update_etf_history_data()
    update_trade_date()
    update_ratation()
    send_ratation_message()
    update_rotation_rank()
    get_portfolio_report()


if __name__ == "__main__":
    run_every_day()
    