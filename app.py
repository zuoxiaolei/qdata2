import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import datetime
from mysql_util import get_connection, get_max_date

is_local = False
ttl = 600
height = 740
width = 800
index_name = 'slope'
columns = ['code', 'name', 'scale',
           'date', 'price', 'slope',
           'last_slope', 'slope_low',
           'slop_high', 'signal']
code = st.text_input('股代码/ETF基金代码', '159915')
max_date = get_max_date(n=1)
with get_connection() as cursor:
    sql = f'''
    select *
    from (
    select *
    from etf.ads_etf_strategy_history_rpt
    where code='{code}' and date<'{max_date}'
    union all 
    select *
    from etf.ads_etf_strategy_rt_rpt
    where code='{code}' and date='{max_date}'
    ) t 
    order by date desc
    limit 20
    '''
    cursor.execute(sql)
    select_stock_df = cursor.fetchall()
select_stock_df = pd.DataFrame(select_stock_df, columns=columns)
select_stock_df = select_stock_df.drop(['last_slope', 'signal'], axis=1)
st.dataframe(select_stock_df, height=height, hide_index=True, width=width)
if len(select_stock_df) > 0:
    x = [datetime.datetime.strptime(ele, '%Y-%m-%d') for ele in select_stock_df.date]
    y = select_stock_df[index_name]
    plt.plot(x, y)
    plt.xticks(rotation=45)
    plt.title(code)
    st.pyplot(plt.gcf())

st.markdown("## 自选股票/基金")
with get_connection() as cursor:
    sql = '''
    select *
    from etf.ads_etf_strategy_rt_rpt
    where date in (select date from etf.dim_etf_trade_date where rn=1) 
          and code in (select code from ads_etf_subscribe)
              and slope_low is not null
    order by scale desc
    '''
    cursor.execute(sql)
    self_select_df = cursor.fetchall()
self_select_df = pd.DataFrame(self_select_df, columns=columns)
self_select_df = self_select_df.sort_values(index_name)
st.dataframe(self_select_df, height=390, hide_index=True)

st.markdown("## rsrs策略推荐")
with get_connection() as cursor:
    sql = '''
    select *
    from etf.ads_etf_strategy_rt_rpt
    where date in (select date from etf.dim_etf_trade_date where rn=1) 
              and slope_low is not null
                and `signal` is not null
    order by scale desc
    '''
    cursor.execute(sql)
    buy_sell_df = cursor.fetchall()
buy_sell_df = pd.DataFrame(buy_sell_df, columns=columns)
buy_df = buy_sell_df[buy_sell_df.signal == 'buy']
sell_df = buy_sell_df[buy_sell_df.signal == 'sell']
st.dataframe(buy_df, hide_index=True)
st.dataframe(sell_df, hide_index=True)
