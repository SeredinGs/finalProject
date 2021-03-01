'''Генератор данных. Генеририрует исторические и "живые" данные.'''
# TODO продумать ООП реализацию

from datetime import timedelta, datetime
import time
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, Integer, DateTime, table, dialects, column

# Коннектимся к ПОСТгресу
ENGINE = create_engine('postgresql://back:123@192.168.1.24:5432/course')
CONN = ENGINE.connect()

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


# пишем в postgres
# история пишется примерно 20 минут
DF_HIST.to_sql(
    name='transactions',
    schema='course',
    con=ENGINE,
    if_exists='replace',
    index=True,
    index_label='date',
    dtype={
        'date': DateTime(),
        'user': Integer(),
        'event': Integer(),
        'sum': Integer()})

print('История записана')

# Пишем актуальные данные
for row in DF_FUT.to_dict('records'):
    time.sleep(10)
    a = dialects.postgresql.insert(TR, values=row)
    CONN.execute(a)
