import argparse
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from dags.configura_const import COMPUTE_FILE_PATH, COMPUTE_FILE_URI



def taxi_trip_compute(data_source: str, output_uri: str) -> None:
    
    appName = "spark-101-{}".format(datetime.today())
    with SparkSession.builder.appName(appName).getOrCreate() as spark:
        # Load CSV file
        df: DataFrame = spark.read \
                .option("header", "True") \
                .option("inferSchema", "True") \
                .parquet(data_source)
                
        # Log into EMR stdout
        print(f"Number of rows and columns: {df.count()}, {len(df.columns)}")
        
        # Create an in-memory DataFrame
        df.createOrReplaceTempView("taxi_trip_202401")
        
        # Group by Amount by vendor_id
        AMOUNT_BY_VENDORID = """
            WITH amount_by_vendor AS (
                
                SELECT 
                    vendor_id
                    , SUM(*) AS sum_amount_by_vendor
                FROM taxi_trip_202401
                GROUP BY vendor_id
                
            ), amount_by_day (
                
                SELECT 
                    day
                    , sum(*) AS sum_amount_by_day
                FROM amount_by_vendor
                WHERE day BETWEEN 1 IN 31
                GROUP BY day
                    
            )
            
            SELECT * FROM amount_by_day
        """
        
        # Compute data
        compute_df = spark.sql(AMOUNT_BY_VENDORID)
        
        # Log into EMR stdout
        print(f"Number of rows and columns: {df.count()}, {len(df.columns)}")
        
        # Write our results as parquet files
        compute_df.write.mode("overwrite").parquet(output_uri)
        
        
taxi_trip_compute(COMPUTE_FILE_PATH, COMPUTE_FILE_URI)