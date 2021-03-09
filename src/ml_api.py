from pyspark.ml.classification import LogisticRegressionModel as mdl
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark import SparkContext


def payload(json):
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    sc = SparkContext.getOrCreate()
    modelka = mdl.load('./models/amounts.model')
    va = VectorAssembler(inputCols=['user','special','amount','percent','term'], outputCol="features")
    df = spark.read.json(sc.parallelize([json]))
    test = va.transform(df)
    pred = modelka.transform(test)
    approved = pred.take(1)[0][-1]
    spark.stop()
    sc.stop()
    return approved