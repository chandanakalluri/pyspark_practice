from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder \
    .appName("OrderNumberExample") \
    .getOrCreate()

data = [("Bob", "Orange"),
        ("Alice", "Apple"),
        ("Alice", "Banana"),
        ("Bob", "Apple"),
        ("Bob", "Banana")]

df = spark.createDataFrame(data, ["Customer", "Product"])

window_spec = Window.partitionBy("Customer").orderBy("Customer")

df_with_order_number = df.withColumn("OrderNumber", row_number().over(window_spec))
df_with_order_number.show()

