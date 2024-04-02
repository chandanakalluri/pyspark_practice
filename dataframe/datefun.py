from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("date functions").getOrCreate()
read_df = spark.read.csv(r"C:\Users\Suresh\Documents\ch\data1.csv", inferSchema=True, header=True)
read_df.show()

df = read_df.withColumn("date", current_date())
df.show()
c_date = read_df.withColumn("timestamo", current_timestamp())
c_date.show()
# add_date=read_df.withColumn("add",add_date("date",7))
# to=read_df.withColumn("adding",to_date("date"))
# to.show()

read_df = read_df.withColumn("date", to_date(read_df["date"], "MM-dd-yyyy"))

read_df.printSchema()

year1 = read_df.withColumn("Year", year(read_df.date))
year1.show()
day1 = read_df.withColumn("Day", day("date"))
day1.show()
month1 = read_df.withColumn("Month", month("date"))
month1.show()
