from pyspark.sql.types import *

from pyspark_practice.RDD.creating_emptyrdd import spark, emp

schema=StructType([StructField("firstn", StringType(), True ),
                   StructField("Ln", StringType(),True),
                   StructField("sal",IntegerType(), True)
    ])
print(schema)
df = spark.createDataFrame(emp,schema)
df.printSchema()