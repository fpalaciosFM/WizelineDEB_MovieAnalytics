from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import (
    col,
    array,
    lit,
    regexp_replace,
    array_distinct,
    arrays_overlap,
    when,
)

spark = SparkSession.builder.appName("WDEB").getOrCreate()

# read movie_review.csv file into a dataframe
df = spark.read.csv(
    "gs://wizeline-deb-movie-analytics-fpa/movie_review.csv",
    inferSchema="true",
    header="true",
)

# delete non alphanumeric characters from column review_str
df = df.withColumn(
    "review_str", regexp_replace(col("review_str"), "([^a-zA-Z0-9 ])", " $1 ")
)

# apply tokenizer to column review_str to split the reviews into an array of words
df = Tokenizer(inputCol="review_str", outputCol="tokens").transform(df)

# remove duplicate words
df = df.withColumn("tokens", array_distinct(col("tokens")))

# array of stop words
stop_words = []

# remove stop words from column tokens
df = StopWordsRemover(
    stopWords=stop_words, inputCol="tokens", outputCol="tokens2"
).transform(df)

# array of positive words
positive_words = ["good", "great", "excelent"]

# create a new column with contant value the content of the positive_words array
df = df.withColumn("positive_words", array([lit(i) for i in positive_words]))

# check for every row if the arrays in columns tokens2 and positive_words contain at least one word in common
df = df.withColumn(
    "positive_review", arrays_overlap(col("tokens2"), col("positive_words"))
)

# removal of auxiliary columns
df = df.drop("tokens2").drop("tokens").drop("review_str").drop("positive_words")

# replacement of data type in column positive_review, from boolean to integer
df = df.withColumn("positive_review", when(df.positive_review, 1).otherwise(0))

# save file in bucket with parquet format
df.write.parquet(
    "gs://wizeline-deb-movie-analytics-fpa/parquet/movie_review.parquet",
    mode="overwrite",
)
