import os
import json
import boto3
from pathlib import Path
from typing import Any
from datetime import datetime, timedelta

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator
)
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)



S3_FOLDER_INPUT = "input"
S3_FOLDER_OUTPUT = "output"
S3_BUCKET_NAME = "spark-tf-processing-s3"
S3_FILENAME = "Orders.csv"
TEMP_FILE_PATH = "/opt/airflow/data/Orders.csv"
S3_KEY = f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_INPUT}/{S3_FILENAME}"
FILE_PATH = Path(__file__).joinpath("..", "..", "data", "Orders.csv").resolve()

SECURITY_CONFIGURATION: dict[str, dict] = {
    "AuthorizationConfiguration": {
        "IAMConfiguration": {
            "EnableApplicationScopedIAMRole": True,
        },
    },
    "InstanceMetadataServiceConfiguration": {
        "MinimumInstanceMetadataServiceVersion": 2,
        "HttpPutResponseHopLimit": 2,
    },
}

SPARK_STEPS: list[dict[str, Any]] = [
    {
        "Name": "calculate_pi",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "/usr/lib/spark/bin/run-example", 
                "SparkPi", 
                "10"
            ],
        },
    }
]

JOB_FLOW_OVERRIDES: dict[str, Any] = {
    "Name": "Spark-tf-processing-emr",
    "ReleaseLabel": "emr-6.13.0",
    "Applications": [
        {"Name": "Spark"}, 
        {"Name": "JupyterEnterpriseGateway"},
        {"Name": "Zeppelin"}
    ],
    "LogUri": "s3://spark-tf-processing-s3/logs/",
    "VisibleToAllUsers": False,
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 3,
            }
        ],
        
        "Ec2SubnetId": "",
        "Ec2KeyName": "",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "Steps": SPARK_STEPS,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}


@task
def configure_security_config(config_name: str):
    boto3.client("emr").create_security_configuration(
        Name=config_name,
        SecurityConfiguration=json.dumps(SECURITY_CONFIGURATION),
    )
    
@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_security_config(config_name: str):
    boto3.client("emr").delete_security_configuration(
        Name=config_name,
    )

@task
def get_step_id(step_ids: list): return step_ids[0]
def get_dag_id(dag_id: str) -> str:
    print(f"start processing pipeline with DAG ID: {dag_id}")


with DAG(dag_id="dag_processing_pipeline_with_aws_cloud_v02",
         default_args={
             'owner': 'longdata',
             'retries': 3,
             'retry_delay': timedelta(minutes=5),
             'depends_on_past': False,
             'email_on_failure': False,
             'email_on_retry': False,
             'email': ['myemail@domain.com']
         },
         description="""
                    Utilize the AWS cloud computing platform 
                    to process, calculate, and aggregate large 
                    amounts of data, and automate and schedule 
                    tasks using Apache Airflow.
                """,
         start_date=datetime(2024, 6, 26),
         schedule_interval='0 0 * * *',
         catchup=False) as dag:
    
    
    # Init pipeline on airflow
    start_processing_pipeline = PythonOperator(
        task_id="start_processing_pipeline_id",
        python_callable=get_dag_id,
        op_kwargs={
            "dag_id": "{{ dag.dag_id }}"
        }
    )

    # Load csv file from local machine to s3 bucket
    csv_local_to_s3 = LocalFilesystemToS3Operator(
        task_id="csv_local_to_s3_id",
        aws_conn_id="s3_bucket_connection",
        filename=TEMP_FILE_PATH,
        dest_key=S3_KEY,
        replace=True,
    )
    
    # Create a EMR Cluster
    create_spark_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_spark_emr_cluster_id",
        aws_conn_id=os.getenv("AWS_ACCESS_KEY_ID"),
        emr_conn_id="",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        region_name="ap-southeast-2"
    )
    
    start_processing_pipeline >> [csv_local_to_s3, create_spark_emr_cluster]