import sys
sys.path.append('..')
from src.POSTgressor import postgressor
import numpy as np
import pandas as pd
from sqlalchemy import table, column


# читаем данные по постгресу
dbdata = open('amounts.txt', 'r').readlines()
IP = dbdata[0][:-1]
scheme = dbdata[1]
tabl = dbdata[3]
user = dbdata[4][:-1]
pwd = dbdata[5]

# Структура таблицы. Это нам еще пригодится
TR = table("amounts",
           column("user"),
           column("special"),
           column("amount"),
           column("percent"),
           column("term"),
           column("Approved"),
           schema='course',
           )

dlin = 10000
rng = np.random.default_rng(5000)
np.random.seed(42)

# ID
usrs = rng.integers(low=5000, high=11000, size=dlin)

# Special
spec = rng.integers(low=0, high=2, size=dlin)

# Amount
mon = rng.integers(low=15, high=1000, size=dlin)

# Percent
per = rng.integers(low=4, high=20, size=dlin)

# Term (годы)
term = rng.integers(low=1, high=20, size=dlin)

# Approved
app = np.random.choice(2, dlin,p=[0.47,0.53])

# Собираем всё
df = pd.DataFrame({'user': usrs, 'special': spec, 'amount': mon, 'percent': per, 'term': term, 'Approved': app})

# Разобьем историю выдачи кредитов на 2 части
DF_HIST = df[:8000]
DF_FUT = df[8000:]

# инициализируем модуль записи в ПОСТгресс
worker = postgressor(ip=IP,schema=scheme,dbname=scheme,schemtable=TR,table=tabl, user=user,pwd=pwd)
# пишем историю
worker.writeHist(DF_HIST)
print('История записана')
# пишем текущие данные
input('пресс энтер ту кантинуэ')
worker.writeCurr(DF_FUT)