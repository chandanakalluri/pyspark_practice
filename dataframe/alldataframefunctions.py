from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("dataframefun").getOrCreate()
df=spark.read.csv(r"C:\Users\Suresh\Documents\ch\data.csv",header=True,inferSchema=True)
df.withColumn(colName="custemer",col=col("customer").cast('Integer')).show()
df.withColumn('customer',col('customer')*1).show()
df.withColumn('sal',lit('20000')).show()
df.withColumnRenamed('customer','cid').show()