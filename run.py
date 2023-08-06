import sys
from update_etf_data import run_every_minute, run_every_day
from update_stock_data import run_every_minute as run_stock_every_minute
from update_stock_data import run_every_day as run_stock_every_day

def main():
    method = sys.argv[1]
    other_args = ''
    if len(sys.argv) > 2:
        other_args = sys.argv[2:]
        other_args = ','.join(other_args)
    eval_string = f"{method}({other_args})"
    eval(eval_string)


main()
