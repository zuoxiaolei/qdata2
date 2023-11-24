import pandas as pd
import streamlit as st

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


# def set_self_select():
#     st.write(
#         """<style>
#         [data-testid="stHorizontalBlock"] {
#             align-items: flex-end;
#         }
#         </style>
#         """,
#         unsafe_allow_html=True
#     )
#     col1, col2, col3 = st.columns([2, 1, 1])
#     with col1:
#         code = st.text_input('添加或删除自选的股代码/ETF基金代码')
#     with col2:
#         select_type = st.selectbox("添加/删除", options=['添加', '删除'])
#     with col3:
#         button_status = st.button("提交")
#         if button_status:
#             if select_type == '添加':
#                 with get_connection() as cursor:
#                     cursor.execute(update_subscribe_sql.format(code, code))
#             elif select_type == '删除':
#                 with get_connection() as cursor:
#                     cursor.execute(delete_subscribe_sql.format(code))
#     select_df = mysql_conn.query(select_stock_sql, ttl=0)
#     select_df.columns = ['股票代码', '股票名称']
#     st.dataframe(select_df, hide_index=True, width=400)


def portfolio_strategy():
    weight_data = [['518880', '黄金ETF', 0.4997765583588797],
                   ['512890', '地波红利', 0.29836978509792955],
                   ['159941', '纳指ETF', 0.2018536565431907],
                   ]
    df_weight = pd.DataFrame(weight_data, columns=['code', 'name', 'weight'])
    st.markdown("## 组合投资策略")
    st.dataframe(df_weight, hide_index=True, width=width, height=150)

    # 筛选时间
    sql = '''
    select date, rate 
    from etf.ads_eft_portfolio_rpt
    order by date
    '''
    df_portfolio = mysql_conn.query(sql, ttl=0)
    min_date = df_portfolio.date.min()
    max_date = df_portfolio.date.max()
    options = list(range(int(min_date[:4]), int(max_date[:4]) + 1))[::-1]
    options = [str(ele) for ele in options]
    options = ['all'] + options
    select_year = st.selectbox(label='年份', options=options)
    if select_year != 'all':
        df_portfolio = df_portfolio[df_portfolio.date.map(lambda x: x[:4] == select_year)]
    df_portfolio.index = pd.to_datetime(df_portfolio['date'])
    df_portfolio["profit"] = df_portfolio["rate"]/df_portfolio["rate"].shift() - 1
    accu_returns, annu_returns, max_drawdown, sharpe = calc_indicators(df_portfolio['profit'])
    accu_returns = round(accu_returns, 3)
    annu_returns = round(annu_returns, 3)
    max_drawdown = round(max_drawdown, 3)
    sharpe = round(sharpe, 3)
    options = {
        "xAxis": {
            "type": "category",
            "data": df_portfolio['date'].tolist(),
        },
        "yAxis": {"type": "value"},
        "series": [
            {"data": df_portfolio['rate'].tolist(), "type": "line"}
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

    df_portfolio["profit"] = df_portfolio["profit"].map(lambda x: str(round(100*x, 3))+"%")
    df_portfolio = df_portfolio[['date', 'profit']]
    df_portfolio = df_portfolio.sort_values("date", ascending=False)
    df_portfolio.columns = ['日期', '收益率']
    df_portfolio = df_portfolio.head(100)
    st.dataframe(df_portfolio, hide_index=True, width=width, height=height)

def ratation_strategy():
    sql = '''
    select * 
    from  etf.ads_etf_ratation_strategy
    order by end_date desc
    '''
    df = mysql_conn.query(sql, ttl=0)
    df.columns = ['股票代码', '股票名称', '买入价格', '卖出价格', '买入日期', '卖出日期', '收益率']
    st.markdown("## 股票轮动策略")
    df['收益率'] = df['收益率'].map(lambda x: str(round(x * 100, 2)) + "%")

    st.dataframe(df, hide_index=True, width=width, height=height)

    df_rank = mysql_conn.query(
        """select * from etf.ads_etf_ratation_rank order by date desc limit 20""")
    st.markdown("## 每天股票动量排名")
    st.dataframe(df_rank, hide_index=True, width=width, height=height)

    st.markdown("## 回测曲线")
    max_date = df['卖出日期'].max()
    min_date = df['买入日期'].min()
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
    where code in (select distinct code from etf.ads_etf_ratation_strategy)
    )t
    )t2
    on t1.date=t2.date and t1.code=t2.code
    right join etf.dim_etf_trade_date t3 
           on t1.date=t3.date
    where t3.date>='2015-08-14'
    order by t3.date
    '''
    df_profit = mysql_conn.query(sql)
    options = list(range(int(min_date[:4]), int(max_date[:4]) + 1))[::-1]
    options = [str(ele) for ele in options]
    options = ['all'] + options
    select_year = st.selectbox(label='年份', options=options)
    if select_year != 'all':
        df_profit = df_profit[df_profit.date.map(lambda x: x[:4] == select_year)]
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

    st.markdown("## 策略每年累计收益")
    sql = '''
    select substr(end_date, 1, 4) year_code, concat(round(sum(rate)*100, 2), '%') rate
    from etf.ads_etf_ratation_strategy
    group by substr(end_date, 1, 4)
    order by year_code desc
    '''
    df_year_profit = mysql_conn.query(sql)
    df_year_profit.columns = ['年份', '总收益']
    st.dataframe(df_year_profit, hide_index=True, width=width)


def calc_indicators(df_returns):
    accu_returns = empyrical.cum_returns_final(df_returns)
    annu_returns = empyrical.annual_return(df_returns)
    max_drawdown = empyrical.max_drawdown(df_returns)
    sharpe = empyrical.sharpe_ratio(df_returns)
    return accu_returns, annu_returns, max_drawdown, sharpe


page_names_to_funcs = {
    "轮动策略": ratation_strategy,
    "组合投资": portfolio_strategy,
    # "设置自选": set_self_select,
}
demo_name = st.sidebar.selectbox("选择页面", page_names_to_funcs.keys())
page_names_to_funcs[demo_name]()
