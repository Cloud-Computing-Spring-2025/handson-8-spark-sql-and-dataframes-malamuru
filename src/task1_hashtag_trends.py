from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, lower, trim

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# Split Hashtags column by comma or whitespace, flatten the array, clean and count
hashtag_counts = (
    posts_df
    .withColumn("hashtag", explode(split(col("Hashtags"), "[,\\s]+")))
    .withColumn("hashtag", trim(lower(col("hashtag"))))  # Normalize (optional but useful)
    .filter(col("hashtag") != "")  # Filter out empty strings
    .groupBy("hashtag")
    .count()
    .orderBy(col("count").desc())
)

# Select Top 10
top_hashtags = hashtag_counts.limit(10)

# Save result
top_hashtags.coalesce(1).write.mode("overwrite").csv("outputs/task1_hashtag_trends.csv", header=True)
