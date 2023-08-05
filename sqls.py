max_date_sql = '''
                select date 
                from etf.dim_etf_trade_date
                where rn=1
               '''

stock_index_last = '''
select *
from (
select *
from etf.ads_etf_strategy_history_rpt
where code='{}' and date<'{}'
union all 
select *
from etf.ads_etf_strategy_rt_rpt
where code='{}' and date='{}'
) t 
order by date desc
limit 20
'''

self_select_sql = '''
select *
from etf.ads_etf_strategy_rt_rpt
where date in (select date from etf.dim_etf_trade_date where rn=1) 
      and code in (select code from ads_etf_subscribe)
          and slope_low is not null
order by scale desc
'''

recommand_sql = '''
select *
from etf.ads_etf_strategy_rt_rpt
where date in (select date from etf.dim_etf_trade_date where rn=1) 
          and slope_low is not null
            and `signal` is not null
order by scale desc
'''
