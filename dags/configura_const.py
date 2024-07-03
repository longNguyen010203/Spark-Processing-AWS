from pathlib import Path
from typing import Any



S3_FOLDER_LOG = "logs"
S3_FOLDER_INPUT = "input"
S3_FOLDER_OUTPUT = "output"
S3_BUCKET_NAME = "spark-tf-processing-s3"
S3_FILENAME = "Orders.csv"
TEMP_FILE_PATH = "/opt/airflow/data/Orders.csv"
LOG_URI = f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_LOG}/"
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
        
        "Ec2SubnetId": "",
        "Ec2KeyName": "",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "Steps": SPARK_STEPS,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}