import tqdm
import tushare as ts
from mysql_util import insert_table_by_batch, get_connection
from concurrent.futures import ThreadPoolExecutor

ts.set_token('c911eca61b662e07f7f313603fe50193de90d9ff791c0df63f67196f')
pro = ts.pro_api()


def get_stock_basic_info():
    data = pro.query('stock_basic', exchange='',
                     list_status='L',
                     fields='ts_code,symbol,name,area,industry,list_status,list_date')
    data = data.values.tolist()
    sql = '''
    replace into tushare.stock_basic
    values (%s, %s,%s, %s,%s, %s, %s)
    '''
    insert_table_by_batch(sql, data)


def get_stock_money_flow(ts_code):
    start_date = '19900101'
    end_date = '29990101'
    df = pro.moneyflow(ts_code=ts_code,
                       start_date=start_date,
                       end_date=end_date)
    sql = '''
    replace into tushare.moneyflow
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    '''
    insert_table_by_batch(sql, df.values.tolist())


def get_all_stock_money_flow():
    sql = '''
    select distinct ts_code from tushare.stock_basic
    '''
    with get_connection() as cursor:
        cursor.execute(sql)
        data = cursor.fetchall()
        ts_codes = [ele[0] for ele in data]
    with ThreadPoolExecutor(10) as executor:
        executor.map(get_stock_money_flow, ts_codes)


def get_zhongzheng1000_history():
    get_date_sql = """
    select substr(replace(date, '-', ''),1,6) date
    from etf.dim_etf_trade_date
    group by substr(replace(date, '-', ''),1,6)
    order by date desc
    """
    zhongzheng1000_code = "000852.SH"
    with get_connection() as cursor:
        cursor.execute(get_date_sql)
        data = cursor.fetchall()
    date_all = [ele[0] for ele in data]
    for date in tqdm.tqdm(date_all):
        start_date = date[:-2] + "01"
        end_date = date[:-2] + "31"
        print(date)
        df = pro.index_weight(index_code=zhongzheng1000_code,
                              start_date=start_date, end_date=end_date)
        sql = """
            replace into tushare.index_weight
            values (%s,%s,%s,%s)
            """
        insert_table_by_batch(sql, df.values.tolist())


if __name__ == '__main__':
    get_zhongzheng1000_history()
