import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import datetime

is_local = False
ttl = 600
height = 740
width = 800
index_name = 'slope_standard'
if is_local:
    qdata_prefix = "data/"
    ttl = 0
else:
    qdata_prefix = "https://ghproxy.com/https://raw.githubusercontent.com/zuoxiaolei/qdata/main/data/"


@st.cache_resource(ttl=ttl)
def load_stock_data():
    df = pd.read_csv(qdata_prefix + 'rsrs.csv', dtype={'code': object})
    return df


@st.cache_resource(ttl=ttl)
def load_fund_data():
    df = pd.read_csv(qdata_prefix + 'rsrs_fund.csv', dtype={'code': object})
    return df


@st.cache_resource(ttl=ttl)
def load_etf_strategy():
    df = pd.read_csv(qdata_prefix + 'etf_strategy.csv', dtype={'code': object})
    return df


@st.cache_resource(ttl=ttl)
def load_best_parameter():
    best_params = pd.read_csv(qdata_prefix + 'best_params.csv', dtype={'code': object})
    return best_params


@st.cache_resource(ttl=ttl)
def load_etf_basic_info():
    df = pd.read_csv(qdata_prefix + "dim/exchang_eft_basic_info.csv", dtype={'基金代码': object})
    df = df[['基金代码', '基金简称']]
    df.columns = ['code', 'name']
    return df


stock_df = load_stock_data()
fund_df = load_fund_data()
etf_strategy_df = load_etf_strategy()
best_params = load_best_parameter()
etf_basic_info = load_etf_basic_info()
etf_strategy_df = etf_strategy_df.merge(etf_basic_info, on="code")

col1, col2 = st.columns(2)
with col1:
    code1 = st.text_input('股代码/ETF基金代码', '159915')
    select_stock_df = stock_df[stock_df.code == code1]
    st.dataframe(select_stock_df, height=height, hide_index=True, width=width)
    if len(select_stock_df) > 0:
        x = [datetime.datetime.strptime(ele, '%Y-%m-%d') for ele in select_stock_df.date]
        y = select_stock_df[index_name]
        plt.plot(x, y)
        plt.title(code1)
        st.pyplot(plt.gcf())

with col2:
    code2 = st.text_input('支付宝基金代码', '162605')
    select_fund_df = fund_df[fund_df.code == code2]
    st.dataframe(select_fund_df, height=height, hide_index=True, width=width)
    if len(select_fund_df) > 0:
        x = [datetime.datetime.strptime(ele, '%Y-%m-%d') for ele in select_fund_df.date]
        y = select_fund_df[index_name]
        plt.figure()
        plt.plot(x, y)
        plt.title(code2)
        st.pyplot(plt.gcf())

st.markdown("## 自选股票/基金")
self_select_codes = ['159941', '512000', '159915',
                     '512880', '603986', '515800',
                     '512100', '510300', '513050', '510210', '512760']
self_select_df = stock_df[stock_df.code.isin(self_select_codes)].groupby('code').tail(1)
self_select_df = self_select_df.sort_values(index_name)
self_select_df = self_select_df.merge(best_params, on=['code'])
st.dataframe(self_select_df, height=390, hide_index=True)

st.markdown("## rsrs策略推荐")
buy_df = etf_strategy_df[etf_strategy_df.signal == 'buy']
sell_df = etf_strategy_df[etf_strategy_df.signal == 'sell']
st.dataframe(buy_df, hide_index=True)
st.dataframe(sell_df, hide_index=True)
