from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Filter Customers with Product").getOrCreate()

# Define schemas
purchase_schema = StructType([
    StructField("customer", IntegerType(), True),
    StructField("product_model", StringType(), True)
])

product_schema = StructType([
    StructField("product_model", StringType(), True)
])

# Sample data
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

# Create DataFrames
purchase_data_df = spark.createDataFrame(data=purchase_data, schema=purchase_schema)
product_data_df = spark.createDataFrame(data=product_data, schema=product_schema)


def filter_and_join_data(purchase_data_df, product_data_df):
    # Grouping purchase data by customer and counting purchases
    filtered_purchase_df = purchase_data_df.groupBy("customer").count()

    # Filtering customers with only one purchase
    count = filtered_purchase_df.filter("count = 1")

    # Joining filtered purchase data with original purchase data and product data
    res_df = count.join(purchase_data_df, "customer").join(product_data_df, "product_model").filter(
        "product_model = 'iphone13'").select("customer")


    res_df.show()

    # Returning the resulting DataFrame
    return res_df


# Example usage:
# Assuming you have 'purchase_data_df' and 'product_data_df' DataFrames already defined
result = filter_and_join_data(purchase_data_df, product_data_df)

