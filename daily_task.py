from etl import etl
from utils.alert import notification
from datetime import datetime

now = datetime.now()
today = datetime(now.year, now.month, now.day)

# ETL for daily candles
notification(
    'ETL (daily) STARTS', '''
             Daily candles ETL starts at {}\n
             Another Message will be sent when completed.
             '''.format(now.strftime('%Y-%m-%d %H:%M:%S')))

etl('1d', today)

# ETL for intraday candles
notification(
    'ETL (intraday) STARTS', '''
             Intraday candles ETL starts at {}\n
             Another Message will be sent when completed.
             '''.format(now.strftime('%Y-%m-%d %H:%M:%S')))

etl('1m', today)
