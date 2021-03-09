''' Модуль прогнозирования выдачи кредита. На вход ML подаются
 агрегаты из hdfs '''
# напоминание:pyspark --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1

from pyspark.ml.clustering import KMeans
# from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark import SparkConf, SparkContext

spark = SparkContext()

df = spark.read.parquet('./transactions/grouped.parquet')

kmeans = KMeans().setK(3).setSeed(1)
va = VectorAssembler(inputCols=['user','summa'], outputCol="features")
feat = va.transform(df)
# train, test = feat.randomSplit([0.7,0.3])
model = kmeans.fit(feat)
# model.save('./models/clusters.model')

pred = model.transform(feat).select('user','prediction')

pred.write.format("mongo").mode("overwrite").option("database",
"course").option("collection", "prediction").option("uri",
"mongodb://127.0.0.1/").save()


"""
# Это еще пригодится
spark.read.format("mongo").option("uri",
"mongodb://127.0.0.1/course.prediction").load()
"""