from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark import SparkConf, SparkContext

spark = SparkContext()

df = spark.read.parquet('./transactions/grouped.parquet')

kmeans = KMeans().setK(3).setSeed(1)
va = VectorAssembler(inputCols=['user', 'summa'], outputCol="features")
feat = va.transform(df)
train, test = feat.randomSplit([0.7,0.3])
model = kmeans.fit(train)
pred = model.transform(test)
evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(pred)
print("Silhouette with squared euclidean distance = " + str(silhouette))

centers = model.clusterCenters()
print("Cluster Centers: ")

for center in centers:
     print(center)