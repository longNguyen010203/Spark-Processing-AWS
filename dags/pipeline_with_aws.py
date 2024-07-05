import os
import json
import boto3
import configura_const
from datetime import datetime, timedelta

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.redshift_cluster import RedshiftCreateClusterOperator
from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator


@task
def configure_security_config(config_name: str):
    boto3.client("emr").create_security_configuration(
        Name=config_name,
        SecurityConfiguration=json.dumps(
            configura_const.SECURITY_CONFIGURATION
        ),
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


with DAG(dag_id="dag_processing_pipeline_with_aws_cloud_v04",
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
        task_id="start_processing_pipeline",
        python_callable=get_dag_id,
        op_kwargs={
            "dag_id": "{{ dag.dag_id }}"
        }
    )
    
    # Create s3 bucket
    create_aws_s3_bucket = S3CreateBucketOperator(
        task_id="create_aws_s3_bucket",
        bucket_name=configura_const.S3_BUCKET_NAME,
        aws_conn_id=os.getenv("AWS_CONN_ID"),
        region_name="ap-southeast-2"
    )

    # Load csv file from local machine to s3 bucket
    csv_local_to_s3 = LocalFilesystemToS3Operator(
        task_id="csv_local_to_s3",
        aws_conn_id=os.getenv("AWS_CONN_ID"),
        filename=configura_const.TEMP_FILE_PATH,
        dest_key=configura_const.S3_KEY,
        encrypt=True,
        replace=True,
    )
    
    # Create a EMR Cluster
    create_spark_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_spark_emr_cluster",
        aws_conn_id=os.getenv("AWS_CONN_ID"),
        emr_conn_id=os.getenv("AWS_EMR_CONN_ID"),
        job_flow_overrides=configura_const.JOB_FLOW_OVERRIDES,
        region_name="ap-southeast-2"
    )
    
    # Check if emr cluster has been created
    is_emr_cluster_created = EmrJobFlowSensor(
        task_id="is_emr_cluster_created",
        target_states={"WAITING"},
        failed_states={"TERMINATED WITH ERRORS"},
        aws_conn_id=os.getenv("AWS_CONN_ID"),
        poke_interval=timedelta(seconds=5),
        timeout=timedelta(seconds=3600),
        max_attempts=2,
        mode="poke",
        job_flow_id="""{{
            task_instance.xcom_pull(
                task_ids='create_spark_emr_cluster', 
                key='return_value'
            ) }}""",
    )
    
    
    add_steps_extraction = EmrAddStepsOperator(
        task_id="add_steps_extraction",
        steps=configura_const.SPARK_STEPS_EXTRACTION,
        aws_conn_id=os.getenv("AWS_CONN_ID"),
        job_flow_id="""{{
            task_instance.xcom_pull(
                task_ids='create_spark_emr_cluster', 
                key='return_value'
            ) }}""",
    )
    
    is_extraction_completed = EmrStepSensor(
        task_id="is_extraction_completed",
        timeout=timedelta(seconds=3600),
        poke_interval=timedelta(seconds=5),
        target_states={"COMPLETED"},
        step_id="""{{ 
            task_instance.xcom_pull(
                task_ids='add_steps_extraction')[0] 
            }}""",
        job_flow_id="""{{
            task_instance.xcom_pull(
                task_ids='create_spark_emr_cluster', 
                key='return_value'
            ) }}""",
    )
    
    transformation_file_to_s3 = LocalFilesystemToS3Operator(
        task_id="transformation_file_to_s3",
        aws_conn_id=os.getenv("AWS_CONN_ID"),
        filename=configura_const.TRANSFORM_FILE_NAME,
        dest_key=configura_const.TRANSFORM_FILE_URI,
        replace=True,
    )
    
    add_transformation_step = EmrAddStepsOperator(
        task_id="add_transformation_step",
        steps=configura_const.SPARK_STEPS_TRANSFORMATION,
        aws_conn_id=os.getenv("AWS_CONN_ID"),
        job_flow_id="""{{
            task_instance.xcom_pull(
                task_ids='create_spark_emr_cluster', 
                key='return_value'
            ) }}""",
    )
    
    is_transformation_completed = EmrStepSensor(
        task_id="is_transformation_completed",
        timeout=timedelta(seconds=3600),
        poke_interval=timedelta(seconds=10),
        target_states={"COMPLETED"},
        step_id="""{{ 
            task_instance.xcom_pull(
                task_ids='add_transformation_step')[0] 
            }}""",
        job_flow_id="""{{
            task_instance.xcom_pull(
                task_ids='create_spark_emr_cluster', 
                key='return_value'
            ) }}""",
    )
    
    terminate_spark_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_spaairflowrk_emr_cluster",
        aws_conn_id=os.getenv("AWS_CONN_ID"),
        job_flow_id="""{{
            task_instance.xcom_pull(
                task_ids='create_spark_emr_cluster', 
                key='return_value'
            ) }}""",
    )
    
    is_emr_cluster_terminated = EmrJobFlowSensor(
        task_id="is_emr_cluster_terminated",
        target_states={"TERMINATED"},
        timeout=timedelta(seconds=3600),
        poke_interval=timedelta(seconds=5),
        mode="poke",
        job_flow_id="""{{
            task_instance.xcom_pull(
                task_ids='create_spark_emr_cluster', 
                key='return_value'
            ) }}""",
    )
    
    create_aws_redshift_cluster = RedshiftCreateClusterOperator(
        task_id="create_aws_redshift_cluster",
        cluster_identifier=configura_const.REDSHIFT_CLUSTER_IDENTIFIER,
        publicly_accessible=False,
        cluster_type="single-node",
        node_type="dc2.large",
        number_of_nodes=1,
        encrypted=True,
        master_username=os.getenv("REDSHIFT_DB_LOGIN"),
        master_user_password=os.getenv("REDSHIFT_DB_PASS"),
        db_name=os.getenv("REDSHIFT_DB_NAME")
    )
    
    wait_cluster_available = RedshiftClusterSensor(
        task_id="wait_cluster_available",
        cluster_identifier=configura_const.REDSHIFT_CLUSTER_IDENTIFIER,
        target_status="available",
        poke_interval=timedelta(seconds=5),
        timeout=timedelta(seconds=60 * 30),
        aws_conn_id=os.getenv("AWS_CONN_ID"),
        mode="poke"
    )
    
    list_files_in_s3 = S3ListOperator(
        task_id="list_files_in_s3",
        aws_conn_id=os.getenv("AWS_CONN_ID"),
        bucket=configura_const.S3_BUCKET_NAME,
        prefix=configura_const.S3_FOLDER_OUTPUT,
        delimiter=configura_const.S3_BUCKET_DELIMITER
    )
    
    copy_files_from_s3 = S3CopyObjectOperator.partial(
        task_id="copy_files_from_s3",
        aws_conn_id=os.getenv("AWS_CONN_ID"),
    ).expand_kwargs(
        list_files_in_s3.output.map(
            lambda x: {
                "source_bucket_key": "",
                "dest_bucket_key": ""
            }
        )
    )
    
    transfer_s3_to_redshift = S3ToRedshiftOperator(
        task_id="transfer_s3_to_redshift",
        redshift_conn_id=configura_const.REDSHIFT_CONN_ID,
        s3_bucket=configura_const.S3_BUCKET_NAME,
        s3_key=configura_const.S3_KEY,
        schema=configura_const.REDSHIFT_SCHEMA,
        table=configura_const.REDSHIFT_TABLE,
        copy_options=["parquet"],
    )
    
    end_processing_pipeline = BashOperator(
        task_id="end_processing_pipeline",
        bash_command="echo End Data Pipeline"
    )
    
    
    start_processing_pipeline >> create_aws_s3_bucket
    start_processing_pipeline >> create_spark_emr_cluster
    create_spark_emr_cluster >> is_emr_cluster_created
    create_aws_s3_bucket >> csv_local_to_s3 >> add_steps_extraction
    is_emr_cluster_created >> add_steps_extraction >> is_extraction_completed
    is_extraction_completed >> add_transformation_step >> is_transformation_completed
    is_transformation_completed >> [list_files_in_s3, transfer_s3_to_redshift]
    create_aws_s3_bucket >> transformation_file_to_s3 >> add_transformation_step
    is_transformation_completed >> terminate_spark_emr_cluster
    terminate_spark_emr_cluster >> is_emr_cluster_terminated
    is_emr_cluster_terminated >> end_processing_pipeline
    start_processing_pipeline >> create_aws_redshift_cluster >> wait_cluster_available
    wait_cluster_available >> transfer_s3_to_redshift
    transfer_s3_to_redshift >> end_processing_pipeline