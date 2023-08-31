import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import datetime
import pymysql
from sqls import *
from mysql_util import get_connection
from streamlit_echarts import st_echarts
import empyrical

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
mysql_conn = st.experimental_connection('mysql', type='sql', ttl=ttl)
max_date = mysql_conn.query(max_date_sql, ttl=ttl).values.tolist()[0][0]


def show_rsrs_strategy():
    code = st.text_input('股代码/ETF基金代码', '159915')
    select_stock_df = mysql_conn.query(
        stock_index_last.format(code, max_date, code, max_date, code, max_date, code, max_date), ttl=ttl)
    select_stock_df.columns = columns
    select_stock_df = select_stock_df.drop(['昨天rsrs指标', '买卖信号'], axis=1)
    st.dataframe(select_stock_df, height=height, hide_index=True, width=width)
    if len(select_stock_df) > 0:
        options = {
            "xAxis": {
                "type": "category",
                "data": select_stock_df['日期'].tolist(),
            },
            "yAxis": {"type": "value"},
            "series": [
                {"data": select_stock_df[index_name].tolist(), "type": "line"}
            ],
            "tooltip": {
                'trigger': 'axis',
                'backgroundColor': 'rgba(32, 33, 36,.7)',
                'borderColor': 'rgba(32, 33, 36,0.20)',
                'borderWidth': 1,
                'textStyle': {
                    'color': '#fff',
                    'fontSize': '12'
                },
                'axisPointer': {
                    'type': 'cross',
                    'label': {
                        'backgroundColor': '#6a7985'
                    }
                },
            }
        }
        st_echarts(options=options)

    buy_sell_df = mysql_conn.query(f'''select * 
                                       from etf.ads_etf_buy_sell_history
                                       where code='{code}' order by start_date desc''')
    st.markdown("## 历史买卖情况")
    st.dataframe(buy_sell_df, hide_index=True, width=width)

    st.markdown("## 自选股票/基金")
    self_select_df = mysql_conn.query(self_select_sql, ttl=ttl)
    self_select_df.columns = columns
    self_select_df = self_select_df.sort_values(index_name)
    st.dataframe(self_select_df, height=390, hide_index=True)

    st.markdown("## rsrs策略推荐")
    select_date = st.date_input("选择日期", value=datetime.datetime.strptime(
        max_date, '%Y-%m-%d'), format="YYYY-MM-DD")
    buy_sell_df = mysql_conn.query(recommand_sql.format(
        str(select_date), str(select_date)), ttl=ttl)
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


def ratation_strategy():
    sql = '''
    select * 
    from  etf.ads_etf_ratation_strategy
    order by end_date desc
    '''
    df = mysql_conn.query(sql, ttl=0)
    df.columns = ['股票代码', '股票名称', '买入价格', '卖出价格', '买入日期', '卖出日期', '收益率']
    st.markdown("## 股票轮动策略")
    st.dataframe(df, hide_index=True, width=width, height=height)

    df_rank = mysql_conn.query(
        """select * from etf.ads_etf_ratation_rank order by date desc limit 20""")
    st.markdown("## 每天股票动量排名")
    st.dataframe(df_rank, hide_index=True, width=width, height=height)

    st.markdown("## 回测曲线")
    sql = '''
    select t3.date, coalesce(t2.profit, 0) profit
    from (
    select t1.date, t2.code, t2.name
    from etf.dim_etf_trade_date t1 
    join etf.ads_etf_ratation_strategy t2
    where t1.date>='2015-08-14' and t1.date>=t2.start_date and t1.date<=t2.end_date
    )t1
    join (
    select code, date, next_close/close-1 profit
    from (
    select code,
                 date,
                 close,
                 lead(close, 1) over(partition by code order by date) next_close
    from etf.ods_etf_history
    where code in ('159941', '518880', '159915', '159633', '516970', '159736',
                               '512690', '515700', '159629', '159928', '512480')
    )t
    )t2
    on t1.date=t2.date and t1.code=t2.code
    right join etf.dim_etf_trade_date t3 
           on t1.date=t3.date
    where t3.date>='2015-08-14'
    order by t3.date
    '''
    df_profit = mysql_conn.query(sql)
    df_profit.index = pd.to_datetime(df_profit['date'])
    accu_returns, annu_returns, max_drawdown, sharpe = calc_indicators(df_profit['profit'])
    accu_returns = round(accu_returns, 3)
    annu_returns = round(annu_returns, 3)
    max_drawdown = round(max_drawdown, 3)
    sharpe = round(sharpe, 3)

    options = {
        "xAxis": {
            "type": "category",
            "data": df_profit['date'].tolist(),
        },
        "yAxis": {"type": "value"},
        "series": [
            {"data": (df_profit['profit'] + 1).cumprod().tolist(), "type": "line"}
        ],
        "tooltip": {
            'trigger': 'axis',
            'backgroundColor': 'rgba(32, 33, 36,.7)',
            'borderColor': 'rgba(32, 33, 36,0.20)',
            'borderWidth': 1,
            'textStyle': {
                'color': '#fff',
                'fontSize': '12'
            },
            'axisPointer': {
                'type': 'cross',
                'label': {
                    'backgroundColor': '#6a7985'
                }
            },
        },
        "title": {
            'text': f'''累计收益: {accu_returns}\n年化收益: {annu_returns}\n最大回撤:{max_drawdown}\n夏普比:{sharpe}''',
            'right': 'left',
            'top': '0px',
        }
    }
    st_echarts(options=options)


def calc_indicators(df_returns):
    names = ['累计收益', '年化收益', '最大回撤', '夏普比']
    accu_returns = empyrical.cum_returns_final(df_returns)
    annu_returns = empyrical.annual_return(df_returns)
    max_drawdown = empyrical.max_drawdown(df_returns)
    sharpe = empyrical.sharpe_ratio(df_returns)
    return accu_returns, annu_returns, max_drawdown, sharpe


page_names_to_funcs = {
    "轮动策略": ratation_strategy,
    # "RSRS策略": show_rsrs_strategy,
    "设置自选": set_self_select
}
demo_name = st.sidebar.selectbox("选择页面", page_names_to_funcs.keys())
page_names_to_funcs[demo_name]()
