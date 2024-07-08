import argparse
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, avg, from_unixtime, when
from pyspark.sql.functions import split, dayofmonth, month, year
from pyspark.sql.types import FloatType

from dags.configura_const import BOOK_REVIEW_SOURCE, BOOK_REVIEW_OUTPUT



def transform_book_reviews(data_source: str, output_uri: str) -> None:
    
    appName = "spark-101-{}".format(datetime.today())
    with SparkSession.builder.appName(appName).getOrCreate() as spark:
        # Load CSV file
        df: DataFrame = spark.read.option("header", "True") \
               .option("inferSchema", "True") \
               .option("multiLine", "true") \
               .option("quote", "\"") \
               .option("escape", "\"") \
               .option("delimiter", ",") \
               .csv(data_source)
        
        # Rename columns
        df = df.select(
            col("Id").alias("id"),
            col("Title").alias("title"),
            col("Price").alias("price"),
            col("User_id").alias("user_id"),
            col("profileName").alias("profile_name"),
            col("review/helpfulness").alias("review_helpfulness"),
            col("review/score").alias("review_score"),
            col("review/time").alias("review_time"),
            col("review/summary").alias("review_summary"),
            col("review/text").alias("review_text")
        )
        
        # Drop Null value in title column
        df = df.dropna(subset=["title"])
        
        # Fill in avg price for missing values in price columns
        avg_price = df.select(avg(col("price"))).first()[0]
        df = df.fillna({"price": avg_price})
        df = df.withColumn("price", round(col("price"), 2))
        
        # Fill in unknown for missing values in columns
        df = df.fillna({"user_id": "unknown"}) \
                .fillna({"profile_name": "unknown"})
        
        
        df = df.withColumn("numerator", split(col("review_helpfulness"), "/").getItem(0).cast("int")) \
               .withColumn("denominator", split(col("review_helpfulness"), "/").getItem(1).cast("int"))
        df = df.withColumn("review_helpfulness_score", col("numerator") / col("denominator"))
        df = df.drop("numerator", "denominator")
        df = df.withColumn("review_helpfulness_score", round(col("review_helpfulness_score"), 2))
        df = df.fillna({"review_helpfulness_score": 0})
        
        # create day, month, year column
        df = df.withColumn("review_time", from_unixtime(col("review_time"), "yyyy-MM-dd"))
        df = df.withColumn("day", dayofmonth(col("review_time"))) \
                .withColumn("month", month(col("review_time"))) \
                .withColumn("year", year(col("review_time")))
        
        # convert month to month letter
        df = df.withColumn("month_letter", 
                    when(col("month") == 1, "January")
                    .when(col("month") == 2, "February")
                    .when(col("month") == 3, "March")
                    .when(col("month") == 4, "April")
                    .when(col("month") == 5, "May")
                    .when(col("month") == 6, "June")
                    .when(col("month") == 7, "July")
                    .when(col("month") == 8, "August")
                    .when(col("month") == 9, "September")
                    .when(col("month") == 10, "October")
                    .when(col("month") == 11, "November")
                    .when(col("month") == 12, "December")
                    .otherwise("Unknown")
                    )
        
        # Drop NA
        df = df.dropna(subset=["review_summary"])
        df = df.dropna(subset=["review_text"])
        
        # Write our results as parquet files
        df.write.mode("overwrite").parquet(output_uri)
        
        
        
transform_book_reviews(BOOK_REVIEW_SOURCE, BOOK_REVIEW_OUTPUT)