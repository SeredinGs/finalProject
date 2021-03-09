''' Модуль прогнозирования выдачи кредита. На вход ML подаются
 агрегаты из hdfs '''
# напоминание:pyspark --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1

from pyspark.ml.classification import LogisticRegression
# from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark import SparkConf, SparkContext

spark = SparkContext()

df = spark.read.parquet('./amounts/grouped.parquet')

(trainingData, testData) = df.randomSplit([0.7, 0.3])


lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8, labelCol='Approved')
va = VectorAssembler(inputCols=['user','special','amount','percent','term'], outputCol="features")
feat = va.transform(trainingData)
test = va.transform(testData)
# Fit the model
lrModel = lr.fit(feat)
lrModel.save('./models/amounts.model')
# pred = lrModel.transform(test)
#


"""
# Это еще пригодится
spark.read.format("mongo").option("uri",
"mongodb://127.0.0.1/course.prediction").load()
"""