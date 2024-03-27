from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("reading").getOrCreate()
df = spark.read.csv(r"C:\Users\Suresh\Documents\ch\detail.csv",header=True,inferSchema=True)
print(df.show())
print(df.columns)
print(df.take(2))