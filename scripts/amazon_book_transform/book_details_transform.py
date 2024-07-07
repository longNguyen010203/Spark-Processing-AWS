import argparse
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType



def transform_book_details(data_source: str, output_uri: str) -> None:
    
    appName = "spark-101-{}".format(datetime.today())
    with SparkSession.builder.appName(appName).getOrCreate() as spark:
        # Load CSV file
        df = spark.read.option("header", "true").csv(data_source)