from datetime import datetime, timedelta
import pandas as pd
from utils.db import Database


def timeframe_gen(start, end, inc, tz):
    '''
    '''
    if start > end:
        raise ValueError('start cannot be greater than end')

    start = start.replace(tzinfo=tz)
    end = end.replace(tzinfo=tz) + timedelta(days=1, seconds=-1)

    while start < end:
        cutoff = min(start + timedelta(days=inc, seconds=-1),
                     end) if inc is not None else end
        yield {
            'start': int(datetime.timestamp(start)),
            'end': int(datetime.timestamp(cutoff))
        }
        start = cutoff + timedelta(seconds=1)


def convert_from_sec(s):
    res = datetime.utcfromtimestamp(s).strftime('%H:%M:%S')
    return res


def next_start_dates(tab_name, conn_str):
    with Database(conn_str) as conn:
        sql = '''
        SELECT symbol, MAX(date_int_key) AS recent_date from {}
        GROUP BY symbol
        '''.format(tab_name)
        recent_dates_df = pd.read_sql(sql, conn.connector)

    recent_dates_df['recent_date'] = recent_dates_df['recent_date'].apply(
        lambda t: datetime.strptime(str(t), '%Y%m%d') + timedelta(days=1))

    return recent_dates_df.set_index('symbol').T.to_dict('list')
