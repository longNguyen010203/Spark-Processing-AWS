from pathlib import Path
from typing import Any


AWS_CONN_ID = "aws_conn"

S3_BUCKET_NAME = "amazon-books-bucket"
S3_FOLDER_ROOT = "amazon-books-reviews"
S3_FOLDER_LOG = "logs"
S3_FOLDER_SCRIPT = "scripts"
SHELL_FILENAME = "ingest.sh"
SHELL_FILE_PATH = f"/opt/airflow/{S3_FOLDER_SCRIPT}/{SHELL_FILENAME}"
S3_KEY_SHELL = f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_ROOT}/{S3_FOLDER_SCRIPT}/{SHELL_FILENAME}"
LOG_URI = f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_ROOT}/{S3_FOLDER_LOG}/"

TRANSFORM_BOOK_REVIEW = ".py"
TRANSFORM_BOOK_DETAIL = ".py"
TRANSFORM_BOOK_REVIEW_PATH = f"/opt/airflow/{S3_FOLDER_SCRIPT}/amazon_book_transform/{TRANSFORM_BOOK_REVIEW}"
TRANSFORM_BOOK_DETAIL_PATH = f"/opt/airflow/{S3_FOLDER_SCRIPT}/amazon_book_transform/{TRANSFORM_BOOK_DETAIL}"
TRANSFORM_RIVIEW_URI = f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_ROOT}/{S3_FOLDER_SCRIPT}/{TRANSFORM_BOOK_REVIEW}"
TRANSFORM_DETAIL_URI = f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_ROOT}/{S3_FOLDER_SCRIPT}/{TRANSFORM_BOOK_REVIEW}"

COMPUTE_BOOKS_NAME = "book_compute.py"
COMPUTE_BOOKS_PATH = f"/opt/airflow/{S3_FOLDER_SCRIPT}/amazon_book_compute/{COMPUTE_BOOKS_NAME}"
COMPUTE_BOOKS_URI = f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_ROOT}/{S3_FOLDER_SCRIPT}/{COMPUTE_BOOKS_NAME}"

DATA = ""
VPC_NAME = ""


S3_REGION_NAME = "ap-southeast-2"


FILE_PATH = Path(__file__).joinpath("..", "..", "data", "Orders.csv").resolve()

REDSHIFT_CONN_ID = ""

REDSHIFT_CLUSTER_IDENTIFIER = ""
REDSHIFT_SCHEMA = "PUBLIC"
REDSHIFT_TABLE = ""


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

SPARK_TRANSFORMATION_REVIEW: list[dict[str, Any]] = [
    {
        "Name": "Transform Reviews Data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                TRANSFORM_RIVIEW_URI
            ],
        },
    }
]

SPARK_TRANSFORMATION_DETAIL: list[dict[str, Any]] = [
    {
        "Name": "Transform Details Data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                TRANSFORM_DETAIL_URI
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
                COMPUTE_BOOKS_URI
            ],
        },
    }
]

JOB_FLOW_OVERRIDES: dict[str, Any] = {
    "Name": "amazon-books-processing-emr",
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
                "InstanceCount": 2,
            }
        ],
        
        "Ec2SubnetId": "subnet-01e07eab1b91cc469",
        "Ec2KeyName": "amazon-books-key",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}