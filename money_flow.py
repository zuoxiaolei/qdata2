import tushare as ts
from mysql_util import  insert_table_by_batch, get_connection
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

get_all_stock_money_flow()
