from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("print schema").getOrCreate()
data=[(1,'chandu','java',20000),(2,'lakshmi','java',10000),(3,'dhina','python',15000)]
schema=('id','name','skills','sal')
df = spark.createDataFrame(data=data,schema=schema)
df.show()
df.select(sum('sal')).show()
df.select(count('id').alias('count')).show()
