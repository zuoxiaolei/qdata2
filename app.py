import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import pymysql
from sqls import *

pymysql.install_as_MySQLdb()

is_local = False
ttl = 600
height = 740
width = 800
index_name = 'rsrs指标'
columns = ['股票代码', '股票名称', '股票规模',
           '日期', '价格', 'rsrs指标',
           '昨天rsrs指标', 'rsrs买入阈值',
           'rsrs卖出阈值', '买卖信号']
code = st.text_input('股代码/ETF基金代码', '159915')
mysql_conn = st.experimental_connection('mysql', type='sql', ttl=60)
max_date = mysql_conn.query(max_date_sql).values.tolist()[0][0]

select_stock_df = mysql_conn.query(stock_index_last.format(code, max_date, code, max_date))
select_stock_df.columns = columns
select_stock_df = select_stock_df.drop(['昨天rsrs指标', '买卖信号'], axis=1)
st.dataframe(select_stock_df, height=height, hide_index=True, width=width)
if len(select_stock_df) > 0:
    x = [datetime.datetime.strptime(ele, '%Y-%m-%d') for ele in select_stock_df['日期']]
    y = select_stock_df[index_name]
    plt.plot(x, y)
    plt.xticks(rotation=45)
    plt.title(code)
    st.pyplot(plt.gcf())

st.markdown("## 自选股票/基金")
self_select_df = mysql_conn.query(self_select_sql)
self_select_df.columns = columns
self_select_df = self_select_df.sort_values(index_name)
st.dataframe(self_select_df, height=390, hide_index=True)

st.markdown("## rsrs策略推荐")
buy_sell_df = mysql_conn.query(recommand_sql)
buy_sell_df.columns = columns
st.markdown("### 买入推荐")
buy_df = buy_sell_df[buy_sell_df["买卖信号"] == 'buy']
st.dataframe(buy_df, hide_index=True)
st.markdown("### 卖出推荐")
sell_df = buy_sell_df[buy_sell_df["买卖信号"] == 'sell']
st.dataframe(sell_df, hide_index=True)
