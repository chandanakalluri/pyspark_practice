from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import rank, dense_rank

spark = SparkSession.builder \
    .appName("SportsAnalytics") \
    .getOrCreate()
data = [("chandu", 100), ("gani", 150), ("bharu", 200), ("David", 250), ("lakshmi", 200)]
df = spark.createDataFrame(data, ["Name", "Score"])

window_spec = Window.orderBy(df["Score"].desc())

df_with_rank = df.withColumn("Rank", rank().over(window_spec))
df_with_dense_rank = df.withColumn("DenseRank", dense_rank().over(window_spec))


df_with_rank.show()

df_with_dense_rank.show()

