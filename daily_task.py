from etl import etl
from utils.alert import notification
from datetime import datetime
import os

now = datetime.now()
today = datetime(now.year, now.month, now.day)

# ETL for daily candles
notification(
    'ETL (daily) STARTS', '''
             Daily candles ETL starts at {}\n
             Another Message will be sent when completed.
             '''.format(now.strftime('%Y-%m-%d %H:%M:%S')))

etl('1d', today)

while os.path.exists('doc/1d_fail_download_list.csv'):
    etl('1d', today, symbol_file='doc/1d_fail_download_list.csv')

# ETL for intraday candles
notification(
    'ETL (intraday) STARTS', '''
             Intraday candles ETL starts at {}\n
             Another Message will be sent when completed.
             '''.format(now.strftime('%Y-%m-%d %H:%M:%S')))

etl('1m', today)

while os.path.exists('doc/1m_fail_download_list.csv'):
    etl('1m', today, symbol_file='doc/1m_fail_download_list.csv')
