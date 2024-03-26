from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("creation of data").getOrCreate()
data=([1,3,4,5,6,7,4,5,6,7,4,3,3])
sc=spark.sparkContext
file1=sc.textFile(r"C:\Users\Suresh\Desktop\New Text Document.txt")
print(file1.collect())