from pyspark.sql import SparkSession

from pyspark_practice.RDD.creating_emptyrdd import emp
from pyspark_practice.dataframe.emptydataframe import schema

df=emp.toDF(schema)