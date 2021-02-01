from utils.timing import timeframe_gen
from utils.config import tz_info, resol, col_names
from datetime import datetime
import time
import pandas as pd


def download_candles_to_csv(symbol, interval, start_date, end_date, client):
    '''
    download candles and write into a csv file
    '''
    increment = 30 if interval == '1m' else None
    timeframes = timeframe_gen(start=start_date,
                               end=end_date,
                               inc=increment,
                               tz=tz_info[interval])
    for tf in timeframes:
        time0 = time.time()
        raw_data = client.stock_candles(symbol, resol[interval], tf['start'],
                                        tf['end'])
        if raw_data['s'] == 'ok':
            del raw_data['s']
            raw_data['symbol'] = symbol
            transform(pd.DataFrame(raw_data), interval, 'doc/tmp_store.csv')

        sleep_time = max(1 - (time.time() - time0), 0)
        time.sleep(sleep_time)


def transform(df, interval, csv_file):
    '''
    transform df and write into a csv file
    '''
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
    df.to_csv(csv_file, mode='a', index=False, header=False)