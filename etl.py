import finnhub
import os
from utils.etl_helpers import single_etl
from utils.config import cred_info, tab_names, col_names, msg_info
from utils.timing import next_start_dates, convert_from_sec
from utils.db import Database
from utils.alert import notification
from datetime import datetime


def etl(interval,
        end_date,
        start_date=None,
        symbol_file='doc/symbol.csv',
        mode='auto',
        api_key=cred_info['api_key'],
        conn_str=cred_info['conn_str'],
        store_file='doc/tmp_store.csv'):
    '''
    '''
    t_start = datetime.now()
    with open(symbol_file, 'r') as syms:
        symbol_list = [sym.strip() for sym in syms.readlines()]
    # Configurate Start Date0
    if mode == 'auto':
        start_dates = next_start_dates(interval, conn_str)
    elif mode == 'mannual':
        start_dates = {sym: [start_date] for sym in symbol_list}
    else:
        raise ValueError('mode can only be auto or mannual!')
    # Start ETL
    n_records = 0
    client = finnhub.Client(api_key)
    for symbol in symbol_list:
        count_list = []
        single_etl(symbol, interval, start_dates[symbol][0], end_date, client,
                   conn_str, store_file, count_list)
        n_records += sum(count_list)
    if os.path.exists(store_file):
        with Database(conn_str) as conn:
            conn.load_to_RDS(store_file, tab_names[interval],
                             col_names[interval])
            print('uploaded')
    t_end = datetime.now()
    # Send Notification
    msg_sent = msg_info['comp'].format(
        tab_names[interval], t_start.strftime('%Y-%m-%d %H:%M:%S'),
        t_end.strftime('%Y-%m-%d %H:%M:%S'),
        convert_from_sec((t_end - t_start).seconds), n_records)
    notification('ETL ({}) Completion'.format(interval), msg_sent)


if __name__ == '__main__':
    with Database(cred_info['conn_str']) as conn:
        conn.ddl('''
        CREATE TABLE IF NOT EXISTS us_equity_daily_finn(
            _id SERIAL UNIQUE,
            symbol varchar(25) NOT NULL,
            date_int_key int NOT NULL,
            open_price numeric,
            close_price numeric,
            high_price numeric,
            low_price numeric,
            volume numeric
            );

        CREATE TABLE IF NOT EXISTS us_equity_1m_finn(
            _id SERIAL UNIQUE,
            symbol varchar(25) NOT NULL,
            date_int_key int NOT NULL,
            open_price numeric,
            close_price numeric,
            high_price numeric,
            low_price numeric,
            volume numeric,
            timestamp varchar(8)
            )
            ''')
    # First Intraday ETL
    start_date = datetime(2020, 2, 4)
    end_date = datetime(2021, 1, 29)
    etl('1m', end_date, start_date, mode='mannual')

    # First Daily ETL
    start_date = datetime(2001, 2, 4)
    end_date = datetime(2021, 1, 29)
    etl('1d', end_date, start_date, mode='mannual')
