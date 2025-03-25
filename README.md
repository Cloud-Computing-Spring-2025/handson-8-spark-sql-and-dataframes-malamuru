# handson-08-sparkSQL-dataframes-social-media-sentiment-analysis

## **Prerequisites**

Before starting the assignment, ensure we have the following software installed and properly configured on our machine:

1. **Python 3.x**:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install pyspark
     ```

3. **Apache Spark**:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

## **Setup Instructions**

### **1. Project Structure**

Ensure your project directory follows the structure below:

```
SocialMediaSentimentAnalysis/
├── input/
│   ├── posts.csv
│   └── users.csv
├── outputs/
│   ├── hashtag_trends.csv
│   ├── engagement_by_age.csv
│   ├── sentiment_engagement.csv
│   └── top_verified_users.csv
├── src/
│   ├── task1_hashtag_trends.py
│   ├── task2_engagement_by_age.py
│   ├── task3_sentiment_vs_engagement.py
│   └── task4_top_verified_users.py
├── docker-compose.yml
└── README.md
```



- **input/**: Contains the input datasets (`posts.csv` and `users.csv`)  
- **outputs/**: Directory where the results of each task will be saved.
- **src/**: Contains the individual Python scripts for each task.
- **docker-compose.yml**: Docker Compose configuration file to set up Spark.
- **README.md**: Assignment instructions and guidelines.

### **2. Running the Analysis Tasks**

You can run the analysis tasks either locally or using Docker.

#### **a. Running Locally**

1. **Navigate to the Project Directory**:
   ```bash
   cd SocialMediaSentimentAnalysis/
   ```

2. **Execute Each Task Using `spark-submit`**:
   ```bash
 
     spark-submit src/task1_hashtag_trends.py
     spark-submit src/task2_engagement_by_age.py
     spark-submit src/task3_sentiment_vs_engagement.py
     spark-submit src/task4_top_verified_users.py
     
   ```

3. **Verify the Outputs**:
   Check the `outputs/` directory for the resulting files:
   ```bash
   ls outputs/
   ```

## **Overview**

In this hands-on assignment, we will use Apache Spark and PySpark DataFrames to analyze a fictional social media platform dataset, containing user posts, likes, retweets, and sentiment scores. The goal is to derive meaningful insights such as hashtag trends, audience engagement patterns, sentiment impact, and influencer reach.

This exercise builds our real-world skills in large-scale data transformation and analysis—key for careers in data science, marketing analytics, and social platform engineering.

## **Objectives**

By completing this assignment, we will learn to:

1. **Load and preprocess structured CSV data using PySpark**
2. **Use DataFrame operations and Spark SQL for transformations**
3. **Join, group, filter, and aggregate data efficiently**
4. **Extract actionable insights related to social media usage**

## **Dataset**

## **Dataset: posts.csv **

You will work with a dataset containing information about **100+ users** who rated movies across various streaming platforms. The dataset includes the following columns:

| Column Name     | Type    | Description                                           |
|-----------------|---------|-------------------------------------------------------|
| PostID          | Integer | Unique ID for the post                                |
| UserID          | Integer | ID of the user who posted                             |
| Content         | String  | Text content of the post                              |
| Timestamp       | String  | Date and time the post was made                       |
| Likes           | Integer | Number of likes on the post                           |
| Retweets        | Integer | Number of shares/retweets                             |
| Hashtags        | String  | Comma-separated hashtags used in the post             |
| SentimentScore  | Float   | Sentiment score (-1 to 1, where -1 is most negative)  |


---

## **Dataset: users.csv **
| Column Name | Type    | Description                          |
|-------------|---------|--------------------------------------|
| UserID      | Integer | Unique user ID                       |
| Username    | String  | User's handle                        |
| AgeGroup    | String  | Age category (Teen, Adult, Senior)   |
| Country     | String  | Country of residence                 |
| Verified    | Boolean | Whether the account is verified      |

---


## **Assignment Tasks**


### **1. Hashtag Trends **

**Objective:**

Identify trending hashtags by analyzing their frequency of use across all posts.

**Tasks:**

- **Extract Hashtags**: Split the `Hashtags` column and flatten it into individual hashtag entries.
- **Count Frequency**: Count how often each hashtag appears.
- **Find Top Hashtags**: Identify the top 10 most frequently used hashtags.


**Outcome:**  
A ranked list of the most-used hashtags and their frequencies.


| Hashtag     | Count |
|-------------|-------|
| #cleanui    | 23    |
| #love       | 22    |
| #social     | 20    |
| #mood       | 19    |
| #ai         | 19    |
| #tech       | 19    |
| #design     | 19    |
| #fail       | 18    |
| #bug        | 18    |
| #ux         | 17    |

---

### **2. Engagement by Age Group**

**Objective:**  
Understand how users from different age groups engage with content based on likes and retweets.

**Tasks:**

- **Join Datasets**: Combine `posts.csv` and `users.csv` using `UserID`.
- **Group by AgeGroup**: Calculate average likes and retweets for each age group.
- **Rank Groups**: Sort the results to highlight the most engaged age group.

**Outcome:**  

| Age Group | Avg Likes  | Avg Retweets|
|-----------|------------|-------------|
| Teen      | 72.25      | 26.09       |
| Adult     | 66.73      | 29.07       |
| Senior    | 60.0       | 27.75       |

---

### **3. Sentiment vs Engagement**

**Objective:**  
Evaluate how sentiment (positive, neutral, or negative) influences post engagement.

**Tasks:**

- **Categorize Posts**: Group posts into Positive (`>0.3`), Neutral (`-0.3 to 0.3`), and Negative (`< -0.3`) sentiment groups.
- **Analyze Engagement**: Calculate average likes and retweets per sentiment category.

**Outcome:**  

| Sentiment | Avg Likes | Avg Retweets |
|-----------|-----------|--------------|
| Neutral   | 79.25     | 25.71        |
| Positive  | 64.29     | 27.29        |
| Negative  | 64.07     | 29.05        |

---

### **4. Top Verified Users by Reach**

**Objective:**  
Find the most influential verified users based on their post reach (likes + retweets).

**Tasks:**

- **Filter Verified Users**: Use `Verified = True` from `users.csv`.
- **Calculate Reach**: Sum likes and retweets for each user.
- **Rank Users**: Return top 5 verified users with highest total reach.

**Outcome:**  


| Username       | Total Reach |
|----------------|-------------|
| @social_queen  | 1796        |
| @calm_mind     | 1443        |
| @meme_lord     | 1232        |
| @stream_bot    | 1116        |
| @critic99      | 766         |




---

