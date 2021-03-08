from sqlalchemy import create_engine, Integer, DateTime, dialects
import time

class postgressor():
    def __init__(self, ip, schema, dbname, schemtable, table, user, pwd, port=5432):
        self.ip = ip
        self.port = port
        self.schema = schema
        self.dbname = dbname
        self.schemtable = schemtable
        self.table = table
        self.user = user
        self.pwd = pwd
        self.eng = create_engine(f'postgresql://{user}:{pwd}@{ip}:{port}/{schema}')
        self.conn = self.eng.connect()

    def writeTrans(self, df):
        '''
        пишем пандасовский датафрейм в postgres
        история пишется примерно 20 минут
        :param df: датафрейм, который мы хотим записать в ПОСТгрес
        '''
        print('Начинаю запись')
        df.to_sql(
            name=self.table,
            schema=self.schema,
            con=self.eng,
            if_exists='replace',
            index=True,
            index_label='date',
            dtype={
                'date': DateTime(),
                'user': Integer(),
                'event': Integer(),
                'sum': Integer()})

    def writeHist(self, df):
        print('Начинаю запись')
        df.to_sql(
            name=self.table,
            schema=self.schema,
            con=self.eng,
            if_exists='replace',
            index=True,
            index_label='date',
            dtype={
                'user': Integer(),
                'special': Integer(),
                'amount': Integer(),
                'percent': Integer(),
                'term': Integer(),
                'Approved': Integer()
                })


    def writeCurr(self, df):
        '''
        Пишем актуальные данные
        :param df: датафрейм, который мы хотим записать в ПОСТгрес
        :return:
        '''
        for row in df.to_dict('records'):
            time.sleep(10)
            a = dialects.postgresql.insert(self.schemtable, values=row)
            self.conn.execute(a)
            print('Записано')