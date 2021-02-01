import os
import psycopg2
from utils.config import cred_info


class Database(object):
    def __init__(self, connection_string):
        '''
        initialize self's fields using connection_string
        :param connection_string: (Str)
        '''
        self.connection_string = connection_string

    def __enter__(self):
        '''
        enters the connection
        '''
        self.connector = psycopg2.connect(self.connection_string)
        self.cursor = self.connector.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        '''
        exits the connection with commit or rollback
        '''
        if exc_type is None:
            self.connector.commit()
        else:
            self.connector.rollback()
        self.connector.close()

    def ddl(self, sql):
        '''
        performs ddl
        :param sql: (Str)
        :return:
        '''
        self.cursor.execute(sql)

    def load_to_RDS(self, csv_file, tab_name, col_names, remove=True):
        '''
        uploads data into table corresponding to different interval
        from temporary csv file and remove it afterwards
        :param interval: 1m or 1d
        :return:
        '''
        with open(csv_file, 'r') as file:
            self.cursor.copy_from(file, tab_name, columns=col_names, sep=',')
        if remove:
            os.remove(csv_file)
