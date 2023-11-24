max_date_sql = '''
                select date 
                from etf.dim_etf_trade_date
                where rn=1
               '''