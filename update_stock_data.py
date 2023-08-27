from concurrent.futures import ThreadPoolExecutor
import tqdm
import akshare as ak
import easyquotation
import retrying
from mysql_util import get_connection, time_cost, get_max_date, insert_table_by_batch
import time
import pytz

thread_num = 100
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


def run_every_minute():
    update_stock_realtime()


def run_every_day():
    update_stock_basic_info()
    update_stock_history_data()


if __name__ == "__main__":
    # run_every_minute()
    # run_every_day()
    start_date = get_max_date(n=1)
    start_date = start_date.replace('-', '')
    print(start_date)
