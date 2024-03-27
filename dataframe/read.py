from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("reading").getOrCreate()
df = spark.read.csv(r"C:\Users\Suresh\Documents\ch\detail.csv",header=True,inferSchema=True)
print(df.show())
print(df.columns)
print(df.take(2))#
df.show(truncate=False)
df.show(2,truncate=False)#prints 2 records and in each record all(Length) values will be printed
df.show(2,truncate=1)#prints only 1 value will be printed
