import argparse
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from dags.configura_const import (
    COMPUTE_BOOKS_URI,
    TRANSFORM_RIVIEW_URI,
    TRANSFORM_DETAIL_URI
)



def compute_agg_books(data_source: dict[str, str], output_uri: str) -> None:
    
    appName = "spark-101-{}".format(datetime.today())
    with SparkSession.builder.appName(appName).getOrCreate() as spark:
        
        # Load CSV file
        book_details: DataFrame = spark.read \
                .option("header", "True") \
                .option("inferSchema", "True") \
                .csv(data_source["first"]) 
                               
        book_reviews: DataFrame = spark.read \
                .option("header", "True") \
                .option("inferSchema", "True") \
                .csv(data_source["second"])
                        
        # Create an in-memory DataFrame
        book_details.createOrReplaceTempView("book_details")
        book_reviews.createOrReplaceTempView("book_reviews")
        
        # aggregate calculation with SQL query
        AGGREGATE_BY_MONTH_QUERY = """
            WITH daily_sales_products AS (
                
                SELECT
                    CAST(order_purchase_timestamp AS DATE) AS daily
                    , product_id
                    , SUM(CAST(payment_value AS FLOAT)) AS sales
                    , COUNT(DISTINCT(order_id)) AS bills
                FROM {{ref("fact_sales")}}
                WHERE order_status = 'delivered'
                GROUP BY
                    CAST(order_purchase_timestamp AS DATE)
                    , product_id
                    
            ), 
            
            daily_sales_categories AS (
                SELECT
                    ts.daily
                    , TO_CHAR(ts."daily", 'YYYY-MM') AS "monthly"
                    , p.product_category_name_english AS category
                    , ts.sales
                    , ts.bills
                    , (ts.sales / ts.bills) AS values_per_bills
                FROM daily_sales_products ts
                JOIN {{ref("dim_products")}} p
                ON ts.product_id = p.product_id
            )

            SELECT
                monthly
                , category
                , SUM(sales) AS total_sales
                , SUM(bills) AS total_bills
                , (SUM(sales) * 1.0 / SUM(bills)) AS values_per_bills
            FROM daily_sales_categories
            GROUP BY
                monthly
                , category
        """
        
        AGGREGATE_BY_CATEGORIES_QUERY = """
            WITH daily_sales_products AS (
                
                SELECT
                    CAST(order_purchase_timestamp AS DATE) AS daily
                    , product_id
                    , SUM(CAST(payment_value AS FLOAT)) AS sales
                    , COUNT(DISTINCT(order_id)) AS bills
                FROM {{ref("fact_sales")}}
                WHERE order_status = 'delivered'
                GROUP BY
                    CAST(order_purchase_timestamp AS DATE)
                    , product_id
                    
                ), 
                
                daily_sales_categories AS (
                    SELECT
                        ts.daily
                        , TO_CHAR(ts.daily, 'YYYY-MM') AS monthly
                        , p.product_category_name_english AS category
                        , ts.sales
                        , ts.bills
                        , (ts.sales / ts.bills) AS values_per_bills
                    FROM daily_sales_products ts
                    JOIN {{ref("dim_products")}} p
                    ON ts.product_id = p.product_id
                )

                SELECT
                    monthly
                    , category
                    , SUM(sales) AS total_sales
                    , SUM(bills) AS total_bills
                    , (SUM(sales) * 1.0 / SUM(bills)) AS values_per_bills
                    FROM daily_sales_categories
                GROUP BY
                    monthly
                    , category
        """
        
        # Transform data
        transformed_df_first: DataFrame = spark.sql(AGGREGATE_BY_MONTH_QUERY)
        transformed_df_second: DataFrame = spark.sql(AGGREGATE_BY_MONTH_QUERY)
        
        # Log into EMR stdout
        print(f"Number of rows in SQL query: {transformed_df_first.count()}")
        print(f"Number of rows in SQL query: {transformed_df_second.count()}")
        
        transformed_df_first.createOrReplaceTempView("transformed_df_first")
        transformed_df_first.createOrReplaceTempView("transformed_df_first")
        
        JOIN_BY_ID_QUERY = """
            WITH amazon_books AS (
                SELECT DISTINCT 
                    i.video_id
                    , i.title
                    , i.channeltitle 
                    , v.categoryname
                    , m.view
                    , m.like as likes
                    , m.dislike
                    , m.publishedat
                    , l.link_video
                    , i.tags
                    , i.thumbnail_link

                FROM {{ source('gold', 'informationvideos') }} i 
                    INNER JOIN {{ source('gold', 'linkvideos') }} l 
                        ON i.video_id = l.video_id 
                    INNER JOIN {{ source('gold', 'videocategory') }} v 
                        ON i.categoryid = v.categoryid 
                    INNER JOIN (
                        SELECT 
                            video_id
                            , MAX(view_count) AS view
                            , MAX(likes) as like
                            , MAX(dislikes) as dislike
                            , MAX(publishedat) as publishedat
                        FROM {{ source('gold', 'metricvideos') }}
                        GROUP BY video_id
                    ) AS m ON i.video_id = m.video_id
            )
        """
        
        # Aggregate data
        aggregate_df: DataFrame = spark.sql(AGGREGATE_BY_MONTH_QUERY)
        
        # Log into EMR stdout
        print(f"Number of rows in SQL query: {aggregate_df.count()}")
        
        # Write our results as parquet files
        aggregate_df.write.mode("overwrite").parquet(output_uri)
        
        
        
compute_agg_books(
    data_source={
        "first": TRANSFORM_DETAIL_URI,
        "second": TRANSFORM_RIVIEW_URI
    },
    output_uri=COMPUTE_BOOKS_URI
)