import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from textblob import TextBlob

load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER')
TOPIC_NAME = os.getenv('TOPIC_NAME')
CASSANDRA_HOST = os.getenv('CASSANDRA_HOST')

def analyze_sentiment(text):
    if not text:
        return 0.0
    return TextBlob(text).sentiment.polarity

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("RedditTechSentiment")
             .config("spark.executor.memory", "512m")
             .config("spark.executor.cores", "1")
             .config("spark.cassandra.connection.host", CASSANDRA_HOST)
             .getOrCreate())

    sentiment_udf = udf(analyze_sentiment, StringType())

    # 1. Read from Kafka
    df = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", KAFKA_BROKER)
          .option("subscribe", TOPIC_NAME)
          .option("startingOffsets", "latest")
          .load())

    # 2. Parse JSON
    # Kafka messages are in `value` as binary
    from pyspark.sql.functions import from_json, schema_of_json
    sample_json = """{"id":"abc","subreddit":"test","title":"hello","selftext":"world","created_utc":1660000000}"""
    json_schema = spark.read.json(spark.sparkContext.parallelize([sample_json])).schema

    parsed_df = df.select(from_json(col("value").cast("string"), json_schema).alias("data")) \
                  .select("data.*")

    # 3. Compute sentiment on combined text (title + selftext)
    from pyspark.sql.functions import concat_ws
    combined_text = concat_ws(" ", col("title"), col("selftext"))
    result_df = parsed_df.withColumn("sentiment", sentiment_udf(combined_text))

    # 4. Write to Cassandra (real-time)
    # We'll store to a keyspace/table we define as reddit.sentiments
    query = (result_df.writeStream
             .format("org.apache.spark.sql.cassandra")
             .option("keyspace", "reddit")
             .option("table", "sentiments")
             .option("checkpointLocation", "/tmp/spark-checkpoints/reddit-sentiment")
             .outputMode("append")
             .start())

    query.awaitTermination()
