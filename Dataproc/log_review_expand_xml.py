from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("WDEB").getOrCreate()

# read csv from gcs
df = spark.read.csv(
    "gs://wizeline-deb-movie-analytics-fpa/log_reviews.csv",
    inferSchema="true",
    header="true",
)

# expand xml column
df = df.selectExpr(
    "id_review",
    *[
        f"xpath(log, 'reviewlog/log/{col}/text()')[0] as {col}"
        for col in ["logDate", "device", "location", "os", "ipAddress", "phoneNumber"]
    ],
)

# rename columns
df = (
    df.withColumn("log_id", col("id_review"))
    .withColumn("log_date", col("logDate"))
    .withColumn("ip", col("ipAddress"))
    .withColumn("phone_number", col("phoneNumber"))
    .withColumn("browser", col("os"))
)

# select required columns
df = df.select(
    "log_id", "log_date", "device", "os", "location", "browser", "ip", "phone_number"
)

# save file in bucket with parquet format
df.write.parquet(
    "gs://wizeline-deb-movie-analytics-fpa/parquet/log_review.parquet",
    mode="overwrite",
)
