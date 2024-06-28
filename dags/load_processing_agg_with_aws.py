from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG, Dataset
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator




S3_FOLDER_NAME = "raw"
S3_BUCKET_NAME = "emr-s3-storage-data"
S3_FILENAME = "Orders.csv"
TEMP_FILE_PATH = "/opt/airflow/data/Orders.csv"
S3_KEY = f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_NAME}/{S3_FILENAME}"
FILE_PATH = Path(__file__).joinpath("..", "..", "data", "Orders.csv").resolve()

with DAG(dag_id="dag_with_etl_pipeline_operators__v24",
         default_args={
             'owner': 'longdata',
             'retries': 3,
             'retry_delay': timedelta(minutes=5),
             'depends_on_past': False,
             'email_on_failure': False,
             'email_on_retry': False
         },
         description="""Practical Apache AirFlow, for 
                        InternShip in Viettel Solution""",
         start_date=datetime(2024, 6, 26),
         schedule_interval='0 0 * * *',
         catchup=True
) as dag:

    csv_local_to_s3 = LocalFilesystemToS3Operator(
        task_id="csv_local_to_s3_id",
        aws_conn_id="s3_bucket_connection",
        filename=TEMP_FILE_PATH,
        dest_key=S3_KEY,
        replace=True,
    )
    
    csv_local_to_s3
    