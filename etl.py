import finnhub
from utils.timing import convert_from_sec, next_start_dates
from utils.config import cred_info, col_names, tab_names, msg_info, log
from utils.alert import notification
from utils.db import Database
from utils.et_fn import download_candles_to_csv
from datetime import datetime
import os


def etl(interval,
        end_date,
        start_date=None,
        symbol_file='doc/symbol.csv',
        mode='auto',
        api_key=cred_info['api_key'],
        conn_str=cred_info['conn_str']):
    '''
    '''
    with open(symbol_file, 'r') as syms:
        symbol_list = [sym.strip() for sym in syms.readlines()]

    if mode == 'auto':
        start_dates = next_start_dates(tab_names[interval], conn_str)
    elif mode == 'mannual':
        start_dates = {sym: [start_date] for sym in symbol_list}
    else:
        raise ValueError('mode can only be auto or mannual!')

    client = finnhub.Client(api_key)
    fail_download_list = []

    start_time = datetime.now()
    for symbol in symbol_list:
        try:
            download_candles_to_csv(symbol, interval, start_dates[symbol][0],
                                    end_date, client)
        except Exception as e:
            fail_download_list.append(symbol + '\n')
            log.warning('Failed to download {} candles of {} due to {}'.format(
                interval, symbol, e))
        print(symbol)
    # get the total number of records updated
    with open('doc/tmp_store.csv', 'r') as tmp_file:
        n_records = len(tmp_file.readlines())
    # upload to rds
    with Database(conn_str) as conn:
        conn.load_to_RDS('doc/tmp_store.csv', tab_names[interval],
                         col_names[interval])
    end_time = datetime.now()

    # Send notification
    comp_msg = msg_info['comp'].format(
        tab_names[interval], start_time.strftime('%Y-%m-%d %H:%M:%S'),
        end_time.strftime('%Y-%m-%d %H:%M:%S'),
        convert_from_sec((end_time - start_time).seconds), n_records)
    if fail_download_list:
        # write fail downloading symbol
        with open('doc/{}_fail_download_list.csv'.format(interval),
                  'w') as fail:
            fail.writelines(fail_download_list)
        # create failed msg
        comp_msg += msg_info['fail'].format(len(fail_download_list),
                                            ''.join(fail_download_list))
    elif os.path.exists('doc/{}_fail_download_list.csv'):
        os.remove('doc/{}_fail_download_list.csv')

    notification(subject='ETL Completion', msg=comp_msg)


if __name__ == '__main__':
    # Initialize tables
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

    # First daily ETL
    start = datetime(2001, 2, 1)
    end = datetime(2021, 1, 28)
    etl('1d', end_date=end, start_date=start, mode='mannual')

    # First 1m ETL
    start = datetime(2020, 2, 1)
    end = datetime(2021, 1, 28)
    etl('1m', end_date=end, start_date=start, mode='mannual')
