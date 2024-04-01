from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("date functions").getOrCreate()
options = {'header': 'true',
           'inferSchema': 'true',
           'delimiter': ','}
path = r"C:\Users\Suresh\Documents\ch\data.csv"


def read_file(format_type, options, path):
    return spark.read.format(format_type).options(**options).load(path)


read_df = read_file("csv", options, path)
read_df.show()

df = read_df.withColumn("date", current_date())
df.show()
c_date = read_df.withColumn("timestamo", current_timestamp())
c_date.show()
# add_date=read_df.withColumn("add",add_date("date",7))

year = read_df.withColumn("Year", year("date"))
year.show()
day = read_df.withColumn("Day", day("date"))
day.show()
month = read_df.withColumn("Month", month("date"))
month.show()
