from utils.timing import timeframe_gen
from utils.db import Database
from utils.config import (tz_info, resol, col_names, tab_names, intra_lim,
                          api_lim_code, log)
from datetime import datetime
import os
import pandas as pd
import finnhub


def convert_candles_to_csv(df, interval, store_file):
    if interval == '1m':
        df['timestamp'] = df['t'].apply(lambda t: datetime.fromtimestamp(
            t, tz=tz_info[interval]).strftime('%H:%M:%S'))

    df['t'] = df['t'].apply(lambda t: int(
        datetime.fromtimestamp(t, tz=tz_info[interval]).strftime('%Y%m%d')))
    df = df.rename(
        {
            't': 'date_int_key',
            'o': 'open_price',
            'c': 'close_price',
            'h': 'high_price',
            'l': 'low_price',
            'v': 'volume'
        },
        axis='columns')

    new_index = col_names[interval]
    df = df[new_index]
    df.to_csv(store_file, mode='a', index=False, header=False)


def single_etl(symbol, interval, start_date, end_date, client, conn_str,
               store_file, count_store):
    '''
    Performs ETL and mutates count_store to store number of records
    '''
    increment = intra_lim if interval == '1m' else None
    timeframes = timeframe_gen(start=start_date,
                               end=end_date,
                               inc=increment,
                               tz=tz_info[interval])
    for tf in timeframes:
        while True:
            try:
                raw_data = client.stock_candles(symbol, resol[interval],
                                                tf['start'], tf['end'])
                if raw_data['s'] == 'ok':
                    del raw_data['s']
                    raw_data['symbol'] = symbol
                    df = pd.DataFrame(raw_data)
                    count_store.append(len(df))
                    convert_candles_to_csv(df, interval, store_file)
                break
            except finnhub.exceptions.FinnhubAPIException as err:
                if err.status_code == api_lim_code:
                    if os.path.exists(store_file):
                        with Database(conn_str) as conn:
                            conn.load_to_RDS(store_file, tab_names[interval],
                                             col_names[interval])
                        print('uploaded')
                else:
                    log.warning(
                        'ETL for {} is interrupted by FinnhubAPIException code:{}'
                        .format(symbol, err.code) + ', attempt to redo')
                continue
            except Exception as err:
                log.warning(
                    'ETL for {} is interrupted due to {}, attempt to redo'.
                    format(symbol, err))
                continue
    print('completed ETL for {} with {} records'.format(
        symbol, sum(count_store)))
