import csv
from itertools import count
from mailcap import show

from pyspark.sql import *
spark=SparkSession.builder.appName("print schema").getOrCreate()
df= spark.read.csv(r"C:\Users\Suresh\Documents\ch\data.csv", header=True)
df.printSchema()
#count functions
print(df.count())#counting no.of records in atable
row=df.count()
cols=len(df.columns)
print(f"printing rows: {row}")
print(f"printing cols: {cols}")
print(f"printing rows and columns: {(row,cols)}")
 #count:
uni=df.distinct().count()
print(uni)#printing unique columns
df.show()

# Show the result
#df.select(count("customer"), count("modelproduct")).show()
df.groupBy(df.customer).count().show()
df.agg({'customer':'count','modelproduct':'count'}).show()
df.createOrReplaceTempView("emp")
spark.sql("SELECT COUNT(*)FROM emp").show()
spark.sql("SELECT customer, COUNT(*) FROM emp GROUP BY customer").show()
spark.sql("SELECT (distinct modelproduct) FROM emp").show()