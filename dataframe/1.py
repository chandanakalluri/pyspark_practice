import unittest
from pyspark_practice.dataframe.ccc import filter_and_join_data

from pyspark.sql import SparkSession
# Assuming your function is in a separate module

class TestFilterAndJoinData(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestFilterAndJoinData") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_filter_and_join_data(self):
        # Sample data
        purchase_data = [
            (1, "iphone13"),
            (2, "iphone13"),
            (3, "iphone13"),
            (4, "iphone14")
        ]

        product_data = [
            ("iphone13",),
            ("iphone14",)
        ]

        # Create DataFrames
        purchase_data_df = self.spark.createDataFrame(data=purchase_data, schema=["customer", "product_model"])
        product_data_df = self.spark.createDataFrame(data=product_data, schema=["product_model"])

        # Call the function
        result_df = filter_and_join_data(purchase_data_df, product_data_df)

        # Assert that the result DataFrame is not empty
        self.assertFalse(result_df.isEmpty(), "Result DataFrame is empty")


if __name__ == '__main__':
    unittest.main()

