wget -O - https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet | 
aws s3 cp - s3://nyc-yellow-tripdata/2024-01/data/raw/yellow_tripdata_2024-01.parquet