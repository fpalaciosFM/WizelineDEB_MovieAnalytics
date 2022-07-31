from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import col, filter, array, lit, regexp_replace, array_distinct, arrays_overlap, when

spark = SparkSession \
       .builder \
       .appName("WDEB") \
       .getOrCreate()

df = spark.read.csv('gs://wizeline-deb-movie-analytics-fpa/movie_review.csv', inferSchema="true", header="true")
df = df.withColumn('review_str', regexp_replace(col('review_str'), '([^a-zA-Z0-9 ])', ' $1 '))

df = Tokenizer(inputCol="review_str", outputCol="tokens").transform(df)
df = df.withColumn('tokens', array_distinct(col('tokens')))

stop_words = []
df = StopWordsRemover(stopWords=stop_words, inputCol='tokens', outputCol='tokens2').transform(df)

positive_words = ['good', 'great', 'excelent']
f_is_positive = lambda x: 1 if x in positive_words else 0

df = df.withColumn('positive_words', array([lit(i) for i in positive_words]))
df = df.withColumn('positive_review', arrays_overlap(col('tokens2'), col('positive_words')))

df = df.drop('tokens2').drop('tokens').drop('review_str').drop('positive_words')

df = df.withColumn('positive_review', when(df.positive_review, 1).otherwise(0))

df.write.parquet('gs://wizeline-deb-movie-analytics-fpa/parquet/movie_review.parquet', mode='overwrite')