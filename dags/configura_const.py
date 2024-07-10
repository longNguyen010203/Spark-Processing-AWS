from pathlib import Path
from typing import Any


AWS_CONN_ID = "aws_conn"

S3_BUCKET_NAME = "yellow-tripdata-nyc"
S3_FOLDER_ROOT = "2024-01"
S3_FOLDER_LOG = "logs"
S3_FOLDER_SCRIPT = "scripts"
S3_FOLDER_DATA = "data"
SHELL_FILENAME = "ingest.sh"
SHELL_FILE_PATH = f"/opt/airflow/{S3_FOLDER_SCRIPT}/{SHELL_FILENAME}"
S3_KEY_SHELL = f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_ROOT}/{S3_FOLDER_SCRIPT}/{SHELL_FILENAME}"
LOG_URI = f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_ROOT}/{S3_FOLDER_LOG}/"

# # s3://nyc-yellow-tripdata/2024-01/data/raw/yellow_tripdata_2024-01.parquet
# RAW_FILE_NAME = "yellow_tripdata_2024-01.parquet"
# RAW_FILE_PATH = f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_ROOT}/{S3_FOLDER_DATA}/raw/{RAW_FILE_NAME}"


TRANSFORM_FILE_NAME = "yellow_tripdata_transform.py"
TRANSFORM_FILE_PATH = f"/opt/airflow/{S3_FOLDER_SCRIPT}/nyc_tripdata_transform/{TRANSFORM_FILE_NAME}"
TRANSFORM_FILE_URI = f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_ROOT}/{S3_FOLDER_SCRIPT}/{TRANSFORM_FILE_NAME}"


DATA_FILE_NAME = "yellow_tripdata_2024-01.parquet"
BOOK_REVIEW_SOURCE = f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_ROOT}/{S3_FOLDER_DATA}/raw/{DATA_FILE_NAME}"
TRANSFORM_DATA_OUTPUT = f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_ROOT}/{S3_FOLDER_DATA}/transform/{DATA_FILE_NAME}"


COMPUTE_FILE_NAME = "yellow_tripdata_compute.py"
COMPUTE_FILE_PATH = f"/opt/airflow/{S3_FOLDER_SCRIPT}/nyc_tripdata_compute/{COMPUTE_FILE_NAME}"
COMPUTE_FILE_URI = f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_ROOT}/{S3_FOLDER_SCRIPT}/{COMPUTE_FILE_NAME}"


COMPUTE_DATA_OUTPUT = f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_ROOT}/{S3_FOLDER_DATA}/compute/{DATA_FILE_NAME}"



DATA = ""
VPC_NAME = ""


S3_REGION_NAME = "eu-west-1"


FILE_PATH = Path(__file__).joinpath("..", "..", "data", "Orders.csv").resolve()

REDSHIFT_CONN_ID = ""

REDSHIFT_CLUSTER_IDENTIFIER = ""
REDSHIFT_SCHEMA = "PUBLIC"
REDSHIFT_TABLE = ""


JOB_FLOW_OVERRIDES: dict[str, Any] = {
    "Name": "nyc-tripdata-emr-cluster",
    "ReleaseLabel": "emr-6.13.0",
    "Applications": [
        {"Name": "Spark"}, 
        {"Name": "JupyterEnterpriseGateway"},
    ],
    "LogUri": LOG_URI,
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
        
        "Ec2SubnetId": "subnet-03327f94e28f3f1e3",
        "Ec2KeyName": "nyc-tripdata-emr-key",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}


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

SPARK_STEPS_EXTRACTION: list[dict[str, Any]] = [
    {
        "Name": "Extract Orders Data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": f"s3://{S3_REGION_NAME}.elasticmapreduce/libs/script-runner/script-runner.jar",
            "Args": [f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_ROOT}/{S3_FOLDER_SCRIPT}/{SHELL_FILENAME}"],
        },
    }
]

SPARK_TRANSFORMATION: list[dict[str, Any]] = [
    {
        "Name": "Transform Yellow Tripdata Data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                TRANSFORM_DATA_OUTPUT
            ],
        },
    }
]


SPARK_STEPS_COMPUTATION: list[dict[str, Any]] = [
    {
        "Name": "Computing books Data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                COMPUTE_DATA_OUTPUT
            ],
        },
    }
]