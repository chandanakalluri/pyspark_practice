from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("select").getOrCreate()
df=spark.read.csv(r"C:\Users\Suresh\Documents\ch\data.csv",header=True,inferSchema=True)
df.show()

df.filter(df.modelproduct == "iphone13").show()
#df.filter(df.modelproduct.like("%phone%")).show()
#df.filter((df.customer>"1")&(df.customer<"4")).show()