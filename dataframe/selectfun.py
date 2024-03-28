from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("selectfun").getOrCreate()
df=spark.read.csv(r"C:\Users\Suresh\Documents\ch\data.csv",header=True,inferSchema=True)
df.select('customer','modelproduct').show()
df.select(df.customer,df.modelproduct).show()
df.select(df["customer"],df["modelproduct"])
df.select (col("customer"),col("modelproduct")).show()
df.select (*df.columns).show()
print(df.columns)
