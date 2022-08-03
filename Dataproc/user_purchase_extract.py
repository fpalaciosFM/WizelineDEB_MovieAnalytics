from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WDEB").getOrCreate()

# read csv from gcs
df = spark.read.json(
    "gs://wizeline-deb-movie-analytics-fpa/user_purchase.json",
)

# save file in bucket with parquet format
df.write.parquet(
    "gs://wizeline-deb-movie-analytics-fpa/parquet/user_purchase.parquet",
    mode="overwrite",
)
