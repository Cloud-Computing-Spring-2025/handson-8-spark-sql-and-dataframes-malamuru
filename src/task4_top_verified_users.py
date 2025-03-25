from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# Filter only verified users
verified_users_df = users_df.filter(col("Verified") == True)

# Join with posts on UserID
verified_posts_df = posts_df.join(verified_users_df, on="UserID")

# Add Reach column (Likes + Retweets)
verified_posts_df = verified_posts_df.withColumn("Reach", col("Likes") + col("Retweets"))

# Calculate total reach per user
reach_df = verified_posts_df.groupBy("Username").agg(
    _sum("Reach").alias("Total_Reach")
)

# Get top 5 verified users by total reach
top_verified = reach_df.orderBy(col("Total_Reach").desc()).limit(5)

# Save result
top_verified.coalesce(1).write.mode("overwrite").csv("outputs/task4_top_verified_users.csv", header=True)
