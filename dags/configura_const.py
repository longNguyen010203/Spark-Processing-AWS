from pathlib import Path
from typing import Any



S3_FOLDER_LOG = "logs"
S3_FOLDER_INPUT = "input"
S3_FOLDER_OUTPUT = "output"
S3_FOLDER_SCRIPT = "scripts"
S3_REGION_NAME = "ap-southeast-2"
S3_BUCKET_NAME = "spark-tf-processing-s3"
S3_FILENAME = "Orders.csv"
S3_BUCKET_DELIMITER = "/"
TRANSFORM_FILE_NAME = ""
TEMP_FILE_PATH = "/opt/airflow/data/Orders.csv"

TRANSFORM_FILE_URI = f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_SCRIPT}/{TRANSFORM_FILE_NAME}"
LOG_URI = f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_LOG}/"
S3_KEY = f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_INPUT}/{S3_FILENAME}"
FILE_PATH = Path(__file__).joinpath("..", "..", "data", "Orders.csv").resolve()

AWS_CONN_ID = "s3_bucket_connection"
REDSHIFT_CONN_ID = ""

EC2_SUBNET_ID = ""
EC2_KEY_NAME = ""
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
            "Args": [f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_SCRIPT}/ingest.sh"],
        },
    }
]

SPARK_STEPS_TRANSFORMATION: list[dict[str, Any]] = [
    {
        "Name": "Transform Orders Data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_SCRIPT}/{TRANSFORM_FILE_NAME}"
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
        
        "Ec2SubnetId": EC2_SUBNET_ID,
        "Ec2KeyName": EC2_KEY_NAME,
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "Steps": SPARK_STEPS_EXTRACTION,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}