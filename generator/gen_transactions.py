'''Генератор данных. Генеририрует исторические и "живые" данные.'''

import sys
sys.path.append('..')
from src.POSTgressor import postgressor
from datetime import timedelta, datetime
import numpy as np
import pandas as pd
from sqlalchemy import table, column


# читаем данные по постгресу
dbdata = open('transactions.txt', 'r').readlines()
IP = dbdata[0][:-1]
scheme = dbdata[1][:-1]
tabl = dbdata[3]
user = dbdata[4][:-1]
pwd = dbdata[5]

# Структура таблицы. Это нам еще пригодится
TR = table("transactions",
           column("date"),
           column("user"),
           column("event"),
           column("sum"),
           schema='course',
           )

# Генерим метки времени
# Предполагается генерить данные на год назад и на месяц вперед
CURRENTTIME = datetime.now()

STARTDATE = CURRENTTIME - timedelta(days=365)
ENDDATE = CURRENTTIME + timedelta(days=31)
DATES = []
i = STARTDATE
# TODO реализовать внутренними средствами, а не Вайлом
while i < ENDDATE:
    i = i + timedelta(seconds=10)
    DATES.append(i)
DATESNP = np.array(DATES, dtype='datetime64')

# Логично предположить, что размер остальных массивов должен совпадать с
# массивом меток времени
DLIN = len(DATES)

# Генерим денежные транзакции. Делаем посев постоянным, чтобы имелся ожидаемый результат.
# Для "внеклассного чтения" предполагается посев убрать.
RNG = np.random.default_rng(5000)
MON = RNG.integers(low=-67, high=70, size=DLIN)

# Генеририм ID-шники для юзеров
USRS = RNG.integers(low=1000, high=10000, size=DLIN)

# Генерим ID события
EVTS = RNG.integers(low=1, high=11, size=DLIN)

# Собираем всё
DF = pd.DataFrame({'user': USRS, 'event': EVTS, 'sum': MON}, index=DATESNP)

# Бьём датафрейм по текущему времени. Всё, что было до - исторические,
# после - текущие
DF_HIST = DF[:CURRENTTIME]
DF_FUT = DF[CURRENTTIME:]

# инициализируем модуль записи в ПОСТгресс
worker = postgressor(ip=IP,schema=scheme,dbname=scheme,schemtable=TR,table=tabl, user=user,pwd=pwd)
# пишем историю
worker.writeTrans(DF_HIST)
print('История записана')
# пишем текущие данные
input('пресс энтер ту кантинуэ')
worker.writeCurr(DF_FUT)
