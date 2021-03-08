'''
Обработчик данных. Читает данные из Postgres, сохраняет агрегированные в hdfs
'''

from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as F

spark = SparkContext()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://192.168.1.24:5432/course") \
    .option("dbtable", "course.transactions") \
    .option("user", "back") \
    .option("password", "123") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df1 = df.withColumn('date_truc', F.date_trunc('day', df.date)).drop('date')

df2 = df1.groupby('user').agg(F.sum('sum').alias('summa'))
df2.repartition('user').write.mode('overwrite').parquet('./transactions/grouped.parquet')


# добавить второй источник хранения агрегатов