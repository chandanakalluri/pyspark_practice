from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("print schema").getOrCreate()
data=[(1,'chandu','java'),(2,'lakshmi','java'),(3,'dhina','python')]
schema=('id','name','skills')
df = spark.createDataFrame(data=data,schema=schema)
df.show()
df.printSchema()
df.withColumn('name',concat(df.id, lit(" "), df.name)).show()
df.withColumn('name',substring(df.name,1,3))
df.withColumn('length',df.name).show()
df.withColumn('lower',lower(df.name)).show()
df.withColumn('upper',upper(df.skills)).show()

