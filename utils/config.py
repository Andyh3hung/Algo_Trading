import os
from pytz import utc
import logging as log

cred_info = {
    'api_key': os.environ['api_key'],
    'conn_str': os.environ['conn_str'],
    'email_addr': os.environ['email_addr'],
    'email_code': os.environ['email_code'],
    'sid': os.environ['sid'],
    'token': os.environ['token'],
    'free_tel': os.environ['free_tel'],
    'personal_tel': os.environ['personal_tel']
}

# intraday candles download lim
intra_lim = 30

# api limit status code
api_lim_code = 429

tz_info = {'1m': None, '1d': utc}

resol = {'1m': 1, '1d': 'D'}

tab_names = {'1m': 'us_equity_1m_finn', '1d': 'us_equity_daily_finn'}

col_names = {
    '1m': [
        'symbol', 'date_int_key', 'open_price', 'close_price', 'high_price',
        'low_price', 'volume', 'timestamp'
    ],
    '1d': [
        'symbol', 'date_int_key', 'open_price', 'close_price', 'high_price',
        'low_price', 'volume'
    ]
}

msg_info = {
    'tmp':
    '{}\n\nHi Andy,\n\n{}\n\nBest regards,\nRobot',
    'comp':
    '''
    The ETL for {} is completed\n
    Time Period: {} to {}\n
    Time Spent: {}\n
    Records Added: {}\n
    '''
}

# logging
log.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                filename='doc/log')
