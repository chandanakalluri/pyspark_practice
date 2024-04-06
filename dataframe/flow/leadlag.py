from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import lag, lead

spark = SparkSession.builder \
    .appName("TemperatureChangeExample") \
    .getOrCreate()

data = [("2022-01-01", 20),
        ("2022-02-01", 22),
        ("2022-03-01", 25),
        ("2022-04-01", 23),
        ("2022-05-01", 21)]

df = spark.createDataFrame(data, ["date", "Temperature"])

window_spec = Window.orderBy("date")
df_with_lag_lead = df.withColumn("PreviousTemperature", lag("Temperature").over(window_spec))
df_with_lag_lag = df.withColumn("NextTemperature", lead("Temperature").over(window_spec))
df_with_lag_lag.show()
df_with_lag_lead.show()
df_with_temperature_change = df.withColumn("TemperatureChange", df["Temperature"] - lag("Temperature").over(window_spec))

df_with_temperature_change.show()

spark.stop()
