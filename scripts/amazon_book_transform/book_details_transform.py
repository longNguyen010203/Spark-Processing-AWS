from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import regexp_replace, mode, to_date
from pyspark.sql.functions import col, mode, median, when

from dags.configura_const import BOOK_DETAIL_SOURCE, BOOK_DETAIL_OUTPUT



def transform_book_details(data_source: str, output_uri: str) -> None:
    
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
               
        # Log into EMR stdout
        print(f"Number of rows and columns: {df.count()}, {len(df.columns)}")
               
        # Rename columns
        df = df.withColumnRenamed("Title", "title") \
            .withColumnRenamed("previewLink", "preview_link") \
            .withColumnRenamed("publishedDate", "published_date") \
            .withColumnRenamed("infoLink", "info_link") \
            .withColumnRenamed("ratingsCount", "ratings_count")
            
        # Drop duplicate
        df = df.dropDuplicates()
        
        # Remove Null value in title column
        df = df.dropna(subset=["title"])
        
        # Fill in unknown for missing values in columns
        df = df.fillna({"description": "unknown"}) \
                .fillna({"authors": "unknown"}) \
                .fillna({"image": "unknown"}) \
                .fillna({"preview_link": "unknown"}) \
                .fillna({"publisher": "unknown"}) \
                .fillna({"info_link": "unknown"}) \
                .fillna({"categories": "unknown"})
        
        # Remove unnecessary characters
        df = df.withColumn("authors", regexp_replace("authors","[\\[\\]'']","")) \
               .withColumn("categories", regexp_replace("categories", "[\\[\\]'']", ""))
        
        # Fill in median value for missing values
        median_rating = df.select(median(col("ratings_count"))).first()[0]
        df = df.fillna({"ratings_count": median_rating})
        
        # Fill in mode value for missing values
        mode_date = df.select(mode(col("published_date"))).first()[0]
        df = df.fillna({"published_date": mode_date})
        

        df = df.withColumn(
            "published_date",
            when(col("published_date").rlike("^\d{4}$"), # Check YYYY format
                to_date(col("published_date"), "yyyy")
            ).otherwise(to_date(col("published_date"), "yyyy-MM-dd")) # Check YYYY-MM-DD format
        )
        
        # Log into EMR stdout
        print(f"Number of rows and columns: {df.count()}, {len(df.columns)}")
        
        # Write our results as parquet files
        df.write.mode("overwrite").parquet(output_uri)
        
        
        
transform_book_details(BOOK_DETAIL_SOURCE, BOOK_DETAIL_OUTPUT)