'''
Обработчик данных. Читает данные из Postgres, сохраняет агрегированные в hdfs
'''

from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as F

spark = SparkContext()

# читаем данные по постгресу
dbdata = open('transactions.txt', 'r').readlines()
IP = dbdata[0][:-1]
scheme = dbdata[1]
tabl = dbdata[3]
user = dbdata[4][:-1]
pwd = dbdata[5]

# Читаем постгрес
df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{IP}:5432/{scheme}") \
    .option("dbtable", f"{scheme}.{tabl}") \
    .option("user", user) \
    .option("password", f'{pwd}') \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Обрабатываем дату
df1 = df.withColumn('date_truc', F.date_trunc('day', df.date)).drop('date')

# Агрегируем данные и сохраняем в паркет
df2 = df1.groupby('user').agg(F.sum('sum').alias('summa'))
df2.repartition('user').write.mode('overwrite').parquet('./transactions/grouped.parquet')