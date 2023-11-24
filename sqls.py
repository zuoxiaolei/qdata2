max_date_sql = '''
                select max(date) date 
                from etf.ods_etf_history
                where rn=1
               '''