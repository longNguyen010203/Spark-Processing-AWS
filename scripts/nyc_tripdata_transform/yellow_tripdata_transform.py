from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import dayofmonth


from dags.configura_const import TAXI_TRIPDATA_SOURCE, TRANSFORM_DATA_OUTPUT



def taxi_trip_transform(data_source: str, output_uri: str) -> None:
    
    appName = "spark-101-{}".format(datetime.today())
    with SparkSession.builder.appName(appName).getOrCreate() as spark:
        # Load CSV file
        df: DataFrame = spark.read \
                .option("header", "True") \
                .option("inferSchema", "True") \
                .parquet(data_source)
                
        df = df.select("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", 
                "passenger_count", "trip_distance", "payment_type", "fare_amount", 
                "mta_tax", "tip_amount", "tolls_amount", "total_amount")
               
        # Log into EMR stdout
        print(f"Number of rows and columns: {df.count()}, {len(df.columns)}")
        
        # Rename columns
        df = df.withColumnRenamed("VendorID", "vendor_id") \
               .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
               .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        
        # Convert datatypes
        df = df.withColumn("passenger_count", df["passenger_count"].cast(IntegerType())) \
               .withColumn("payment_type", df["payment_type"].cast(IntegerType())) \
               .withColumn("pickup_datetime", df["pickup_datetime"].cast("timestamp")) \
               .withColumn("dropoff_datetime", df["dropoff_datetime"].cast("timestamp"))
               
        # Create columns pickup_day and dropoff_day
        df = df.withColumn("pickup_day", dayofmonth("pickup_datetime")) \
               .withColumn("dropoff_day", dayofmonth("dropoff_datetime"))
               
        # Log into EMR stdout
        print(f"Number of rows and columns: {df.count()}, {len(df.columns)}")
        
        # Write our results as parquet files
        df.write.mode("overwrite").parquet(output_uri)
        
        
taxi_trip_transform(TAXI_TRIPDATA_SOURCE, TRANSFORM_DATA_OUTPUT)