'''
Обработчик данных. Читает данные из Postgres, сохраняет агрегированные в hdfs
'''

# pyspark --jars ./lib/postgresql-42.2.19.jar

from pyspark import SparkConf, SparkContext

spark = SparkContext()

# читаем данные по постгресу
dbdata = open('amounts.txt', 'r').readlines()
IP = dbdata[0][:-1]
scheme = dbdata[1][:-1]
tabl = dbdata[3][:-1]
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

# сохраняем в паркет
df.repartition('user').write.mode('overwrite').parquet('./amounts/grouped.parquet')
