import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType



def transform_data(data_source: str, output_uri: str) -> None:
    
    appName = "spark-101-{}".format(datetime.today())
    with SparkSession.builder.appName(appName).getOrCreate() as spark:
        # Load CSV file
        df = spark.read.option("header", "true").csv(data_source)
        
        # Rename columns
        df = df.select(
            col("Name").alias("name"),
            col("Violation Type").alias("violation_type"),
        )
        
        # Create an in-memory DataFrame
        df.createOrReplaceTempView("restaurant_violations")
        
        # Construct SQL query
        GROUP_BY_QUERY = """
            SELECT name, count(*) AS total_red_violations
            FROM restaurant_violations
            WHERE violation_type = "RED"
            GROUP BY name
        """
        
        # Transform Data
        transformed_df = spark.sql(GROUP_BY_QUERY)
        
        # Log into EMR stdout
        print(f"Number of rows in SQL query: {transformed_df.count()}")
        
        # Write our results as parquet files
        transformed_df.write.mode("overwrite").parquet(output_uri)
        
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_source")
    parser.add_argument("--output_uri")
    args = parser.parse_args()
    
    transform_data(args.data_source, args.output_uri)