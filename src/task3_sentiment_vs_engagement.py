from pyspark.sql import SparkSession
from pyspark.sql.functions import when, avg, col

spark = SparkSession.builder.appName("SentimentVsEngagement").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)

# Categorize sentiment
posts_with_sentiment = posts_df.withColumn(
    "SentimentCategory",
    when(col("SentimentScore") > 0.3, "Positive")
    .when(col("SentimentScore") < -0.3, "Negative")
    .otherwise("Neutral")
)

# Group by sentiment category and calculate average likes and retweets
sentiment_stats = posts_with_sentiment.groupBy("SentimentCategory").agg(
    avg("Likes").alias("Avg_Likes"),
    avg("Retweets").alias("Avg_Retweets")
)

# Sort by Avg_Likes descending
sentiment_stats = sentiment_stats.orderBy(col("Avg_Likes").desc())

# Save result
sentiment_stats.coalesce(1).write.mode("overwrite").csv("outputs/task3_sentiment_vs_engagement.csv", header=True)
