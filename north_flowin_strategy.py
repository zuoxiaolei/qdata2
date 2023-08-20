'''效果较差'''
import pandas as pd

from mysql_util import get_connection

sql = '''
select t1.code, t1.date, t1.close, t1.momentum_signal, t2.flowin_signal
from (
select code, date, close, 
			 close/close_20-1 increase_rate,
			 case when close/close_20-1>=0.05 then 'buy'
			      when close/close_20-1<=-0.05 then 'sell'
			 else 'hold' end momentum_signal
from (
select code, date, close,
       lag(close, 20) over(partition by code order by date) close_20
from etf.ods_etf_history
where code='159915'
) t
)t1 
join (
select date, net_flowin, 
			 case when net_flowin>flowin_high then 'buy'
			      when net_flowin<flowin_low then 'sell'
						else 'hold'
			 end flowin_signal
from (
select date,
		   net_flowin,
			 avg_net_flowin+1.5*std_net_flowin flowin_high,
			 avg_net_flowin-1.5*std_net_flowin flowin_low
from (
select date, net_flowin, 
		   avg(net_flowin) over(ORDER BY date rows between 251  preceding and current row) avg_net_flowin,
			 std(net_flowin) over(ORDER BY date rows between 251 preceding and current row) std_net_flowin
from stock.ods_stock_north_flowin 
) t 
)t
)t2
on t1.date=t2.date
order by t1.date
'''

with get_connection() as cursor:
    cursor.execute(sql)
    data = cursor.fetchall()


# data = pd.DataFrame(data, columns=['code', 'date', 'close',
#                                    'momentum_signal', 'flowin_signal'])


def get_straget_profit(data):
    length = len(data)
    i = 0
    result = []
    while i < length:
        code, date, close, momentum_signal, flowin_signal = data[i]
        if flowin_signal == 'buy':
            buy_price = close
            buy_date = date
            j = i + 1
            while j < length:
                code, date, close, momentum_signal, flowin_signal = data[j]
                if flowin_signal == 'sell':
                    sell_price = close
                    sell_date = date
                    result.append([buy_price, buy_date, sell_price, sell_date, sell_price / buy_price - 1])
                    i = j
                    break
                j += 1
        i += 1
    return result


result = get_straget_profit(data)
result = pd.DataFrame(result, columns=['buy_price', 'buy_date', 'sell_price', 'sell_date', 'rate'])
result.to_csv('temp.csv', index=False)
result["year"] = result['buy_date'].map(lambda x: x[:4])
print(result.groupby("year")['rate'].sum())
