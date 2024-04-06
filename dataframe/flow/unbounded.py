from pyspark.sql import SparkSession
from pyspark.sql import Window
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("WindowFunctionsExample") \
    .getOrCreate()

data = [("12-1-2024", 100),
        ("13-1-2024", 150),
        ("14-1-2024", 200),
        ("15-1-2024", 250),
        ("16-1-2024", 300),
        ("17-1-2024", 350),
        ("18-1-2024", 400),
        ("19-1-2024", 450)]

df = spark.createDataFrame(data, ["date", "Amount"])

window_spec = Window.partitionBy("date").orderBy("Amount")



# Unbounded Preceding
unbounded_preceding = df.withColumn("TotalAmount_UnboundedPreceding", F.sum("Amount").over(Window.orderBy("Amount")\
                                 .rowsBetween(Window.unboundedPreceding, Window.currentRow)))

# Unbounded Following
unbounded_following = df.withColumn("TotalAmount_UnboundedFollowing", F.sum("Amount").over(Window.orderBy("Amount")\
                                .rowsBetween(Window.currentRow, Window.unboundedFollowing)))

# N Preceding
n_preceding = df.withColumn("TotalAmount_2Preceding", F.sum("Amount")\
                                    .over(Window.orderBy("Amount").rowsBetween(-2, 0)))

# N Following
n_following = df.withColumn("TotalAmount_2Following", F.sum("Amount")\
                                    .over(Window.orderBy("Amount").rowsBetween(0, 2)))




unbounded_preceding.show()

unbounded_following.show()

n_preceding.show()

n_following.show()

