from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Filter Customers with Product").getOrCreate()
purchase_schema = StructType([
    StructField("customer", IntegerType(), True),
    StructField("product_model", StringType(), True)
])

product_schema = StructType([
    StructField("product_model", StringType(), True)
])
purchase_data = [
    (1, "iphone13"),
    (1, "dell i5 core"),
    (2, "iphone13"),
    (2, "dell i5 core"),
    (3, "iphone13"),
    (3, "dell i5 core"),
    (1, "dell i3 core"),
    (1, "hp i5 core"),
    (1, "iphone14"),
    (3, "iphone14"),
    (4, "iphone13")
]

product_data = [
    ("iphone13",),
    ("dell i5 core",),
    ("dell i3 core",),
    ("hp i5 core",),
    ("iphone14",)
]

purchase_data_df = spark.createDataFrame(data=purchase_data, schema=purchase_schema)
product_data_df = spark.createDataFrame(data=product_data, schema=product_schema)

filtered_purchase_df=purchase_data_df.groupBy("customer").count()
count=filtered_purchase_df.filter("count=1")
res_df=count.join(purchase_data_df,"customer").filter("product_model=='iphone13'")


#
filtered_purchase_df.show()
count.show()
res_df.show()



