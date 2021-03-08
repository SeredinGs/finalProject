# заготовка для API

from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import PipelineModel
from pyspark import SparkConf, SparkContext

spark = SparkContext()

df1 = spark.createDataFrame(
    [
        (1353, 1347),
    ],
    ['user', 'summa']
)

va = VectorAssembler(inputCols=['user', 'summa'], outputCol="features")

modelka = KMeansModel.load('./models/clusters.model')

result = modelka.transform(va.transform(df1)).select('prediction').take(1)[0][0]