from pathlib import Path
from datetime import datetime, timedelta
from tasks.processing_logging import sumFraction

from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator
)




S3_FOLDER_NAME = "input"
S3_BUCKET_NAME = "spark-tf-processing-s3"
S3_FILENAME = "Orders.csv"
TEMP_FILE_PATH = "/opt/airflow/data/Orders.csv"
S3_KEY = f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_NAME}/{S3_FILENAME}"
FILE_PATH = Path(__file__).joinpath("..", "..", "data", "Orders.csv").resolve()


with DAG(dag_id="dag_processing_pipeline_with_aws_v01",
         default_args={
             'owner': 'longdata',
             'retries': 3,
             'retry_delay': timedelta(minutes=5),
             'depends_on_past': False,
             'email_on_failure': False,
             'email_on_retry': False
         },
         description="""
                    Utilize the AWS cloud computing platform 
                    to process, calculate, and aggregate large 
                    amounts of data, and automate and schedule 
                    tasks using Apache Airflow.
                """,
         start_date=datetime(2024, 6, 26),
         schedule_interval='0 0 * * *',
         catchup=False
) as dag:

    csv_local_to_s3 = LocalFilesystemToS3Operator(
        task_id="csv_local_to_s3_id",
        aws_conn_id="s3_bucket_connection",
        filename=TEMP_FILE_PATH,
        dest_key=S3_KEY,
        replace=True,
    )
    
    computing_fraction = PythonOperator(
        task_id="computing_fraction",
        python_callable=sumFraction,
        op_kwargs={
            "fraction": {
                "tu_1": 45,
                "mau_1": 33,
                "tu_2": 55,
                "mau_2": 63
            }
        }
    )
    
    computing_fraction
    