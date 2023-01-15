import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Broadcast logs tidy").getOrCreate()

DIRECTORY = "/opt/spark/data/broadcast_logs"

logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8_sample.CSV"),
    sep="|",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd",
)

logs = logs.withColumn(
    "Duration_seconds",
    (
        F.col("Duration").substr(1, 2).cast("int") * 60 * 60
        + F.col("Duration").substr(4, 2).cast("int") * 60
        + F.col("Duration").substr(7, 2).cast("int")
    ),
)

logs.toDF(*[x.lower() for x in logs.columns]).printSchema()
logs.select(sorted(logs.columns)).printSchema()

logs.show(10, False)

print(logs.dtypes)

logs.printSchema()
