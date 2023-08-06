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
where code='{}' and date<'{}' and date in (select date from etf.dim_etf_trade_date where rn<=20)
union all 
select *
from etf.ads_etf_strategy_rt_rpt
where code='{}' and date='{}'
union all
select *
from stock.ads_stock_strategy_history_rpt
where code='{}' and date<'{}' and date in (select date from etf.dim_etf_trade_date where rn<=20)
union all 
select *
from stock.ads_stock_strategy_rt_rpt
where code='{}' and date='{}'
) t 
order by date desc
limit 20
'''

self_select_sql = '''
select * 
from (
select *
from etf.ads_etf_strategy_rt_rpt
where date in (select date from etf.dim_etf_trade_date where rn=1) 
      and code in (select code from etf.ads_etf_subscribe)
      and slope_low is not null
union all 
select *
from stock.ads_stock_strategy_rt_rpt
where date in (select date from etf.dim_etf_trade_date where rn=1) 
      and code in (select code from etf.ads_etf_subscribe)
      and slope_low is not null
      )t
order by scale desc
'''

recommand_sql = '''
select *
from (
select *
from etf.ads_etf_strategy_rt_rpt
where date = '{}'
      and slope_low is not null
      and `signal` is not null
union all
select *
from etf.ads_etf_strategy_history_rpt
where date = '{}' and date in (select date from etf.dim_etf_trade_date where rn>=2)
      and slope_low is not null
      and `signal` is not null
      ) t 
order by scale desc
'''

select_stock_sql = '''
select code, name
from etf.ads_etf_subscribe
'''

update_subscribe_sql = '''
replace into etf.ads_etf_subscribe(code, name)
select code, name
from etf.dim_etf_basic_info
where code='{}'
union
select code, name
from stock.dim_stock_basic_info
where code='{}'
'''

delete_subscribe_sql ='''
delete from etf.ads_etf_subscribe
where code='{}'
'''