import streamlit as st
import matplotlib.pyplot as plt
import datetime
import pymysql
from sqls import *
from mysql_util import get_connection

pymysql.install_as_MySQLdb()

is_local = False
ttl = 600
height = 740
width = 800
st.set_page_config(layout='wide')
index_name = 'rsrs指标'
columns = ['股票代码', '股票名称', '股票规模',
           '日期', '价格', 'rsrs指标',
           '昨天rsrs指标', 'rsrs买入阈值',
           'rsrs卖出阈值', '买卖信号']
mysql_conn = st.experimental_connection('mysql', type='sql', ttl=60)
max_date = mysql_conn.query(max_date_sql).values.tolist()[0][0]


def show_rsrs_strategy():
    code = st.text_input('股代码/ETF基金代码', '159915')
    select_stock_df = mysql_conn.query(
        stock_index_last.format(code, max_date, code, max_date, code, max_date, code, max_date))
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


def set_self_select():
    st.write(
        """<style>
        [data-testid="stHorizontalBlock"] {
            align-items: flex-end;
        }
        </style>
        """,
        unsafe_allow_html=True
    )
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        code = st.text_input('添加或删除自选的股代码/ETF基金代码')
    with col2:
        select_type = st.selectbox("添加/删除", options=['添加', '删除'])
    with col3:
        button_status = st.button("提交")
        if button_status:
            if select_type == '添加':
                with get_connection() as cursor:
                    cursor.execute(update_subscribe_sql.format(code, code))
            elif select_type == '删除':
                with get_connection() as cursor:
                    cursor.execute(delete_subscribe_sql.format(code))
    select_df = mysql_conn.query(select_stock_sql, ttl=0)
    select_df.columns = ['股票代码', '股票名称']
    st.dataframe(select_df, hide_index=True, width=400)


page_names_to_funcs = {
    "RSRS策略": show_rsrs_strategy,
    "设置自选": set_self_select
}
demo_name = st.sidebar.selectbox("选择页面", page_names_to_funcs.keys())
page_names_to_funcs[demo_name]()
