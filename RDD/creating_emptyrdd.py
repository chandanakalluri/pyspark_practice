from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("empty").getOrCreate()
empty=spark.sparkContext.emptyRDD()
print(empty)
emp=spark.sparkContext.parallelize([])
print(emp)