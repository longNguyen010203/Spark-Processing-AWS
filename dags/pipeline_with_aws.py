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
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
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




def get_dag_id(dag_id: str) -> str:
    print(f"start processing pipeline with DAG ID: {dag_id}")


with DAG(dag_id="dag_processing_pipeline_with_aws_cloud_v21",
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
        aws_conn_id=configura_const.AWS_CONN_ID,
        region_name="ap-southeast-2"
    )

    # Load shell file from local machine to s3 bucket
    shell_local_to_s3 = LocalFilesystemToS3Operator(
        task_id="shell_local_to_s3",
        aws_conn_id=configura_const.AWS_CONN_ID,
        filename=configura_const.SHELL_FILE_PATH,
        dest_key=configura_const.S3_KEY_SHELL,
        encrypt=True,
        replace=True,
    )
    
    # Create folder logs/ in s3 for emr log
    create_object_in_s3 = S3CreateObjectOperator(
        task_id="create_object_in_s3",
        # s3_bucket=configura_const.S3_BUCKET_NAME,
        aws_conn_id=configura_const.AWS_CONN_ID,
        s3_key=configura_const.LOG_URI,
        data=configura_const.DATA,
        replace=True,
        encrypt=True,
    )
    
    # Create an emr cluster
    create_spark_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_spark_emr_cluster",
        aws_conn_id=configura_const.AWS_CONN_ID,
        # emr_conn_id=os.getenv("AWS_EMR_CONN_ID"),
        job_flow_overrides=configura_const.JOB_FLOW_OVERRIDES,
        region_name="ap-southeast-2"
    )
    
    # Check if emr cluster has been created
    is_emr_cluster_created = EmrJobFlowSensor(
        task_id="is_emr_cluster_created",
        target_states={"WAITING"},
        aws_conn_id=configura_const.AWS_CONN_ID,
        poke_interval=timedelta(seconds=5),
        timeout=timedelta(seconds=3600),
        mode="poke",
        job_flow_id="""{{
            task_instance.xcom_pull(
                task_ids='create_spark_emr_cluster', 
                key='return_value'
            ) }}""",
    )
    
    # step extraction in emr cluster
    add_steps_extraction = EmrAddStepsOperator(
        task_id="add_steps_extraction",
        steps=configura_const.SPARK_STEPS_EXTRACTION,
        aws_conn_id=configura_const.AWS_CONN_ID,
        job_flow_id="""{{
            task_instance.xcom_pull(
                task_ids='create_spark_emr_cluster', 
                key='return_value'
            ) }}""",
    )
    
    # Check if emr cluster extract completed
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
    
    # Load transformation review data file to s3 bucket
    transformation_review_file_to_s3 = LocalFilesystemToS3Operator(
        task_id="transformation_review_file_to_s3",
        aws_conn_id=configura_const.AWS_CONN_ID,
        filename=configura_const.TRANSFORM_BOOK_REVIEW_PATH,
        dest_key=configura_const.TRANSFORM_RIVIEW_URI,
        replace=True,
    )
    
    # Load transformation detail data file to s3 bucket
    transformation_detail_file_to_s3 = LocalFilesystemToS3Operator(
        task_id="transformation_detail_file_to_s3",
        aws_conn_id=configura_const.AWS_CONN_ID,
        filename=configura_const.TRANSFORM_BOOK_DETAIL_PATH,
        dest_key=configura_const.TRANSFORM_DETAIL_URI,
        replace=True,
    )
    
    # implement transform review data using emr cluster
    transformation_review_step = EmrAddStepsOperator(
        task_id="transformation_review_step",
        steps=configura_const.SPARK_TRANSFORMATION_REVIEW,
        aws_conn_id=configura_const.AWS_CONN_ID,
        job_flow_id="""{{
            task_instance.xcom_pull(
                task_ids='create_spark_emr_cluster', 
                key='return_value'
            ) }}""",
    )
    
    # implement transform data using emr cluster
    transformation_detail_step = EmrAddStepsOperator(
        task_id="transformation_detail_step",
        steps=configura_const.SPARK_TRANSFORMATION_DETAIL,
        aws_conn_id=configura_const.AWS_CONN_ID,
        job_flow_id="""{{
            task_instance.xcom_pull(
                task_ids='create_spark_emr_cluster', 
                key='return_value'
            ) }}""",
    )
    
    is_transformation_review_completed = EmrStepSensor(
        task_id="is_transformation_review_completed",
        timeout=timedelta(seconds=3600),
        poke_interval=timedelta(seconds=10),
        target_states={"COMPLETED"},
        step_id="""{{ 
            task_instance.xcom_pull(
                task_ids='transformation_review_step')[0] 
            }}""",
        job_flow_id="""{{
            task_instance.xcom_pull(
                task_ids='create_spark_emr_cluster', 
                key='return_value'
            ) }}""",
    )
    
    is_transformation_detail_completed = EmrStepSensor(
        task_id="is_transformation_detail_completed",
        timeout=timedelta(seconds=3600),
        poke_interval=timedelta(seconds=10),
        target_states={"COMPLETED"},
        step_id="""{{ 
            task_instance.xcom_pull(
                task_ids='transformation_detail_step')[0] 
            }}""",
        job_flow_id="""{{
            task_instance.xcom_pull(
                task_ids='create_spark_emr_cluster', 
                key='return_value'
            ) }}""",
    )
    
    terminate_spark_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_spark_emr_cluster",
        aws_conn_id=configura_const.AWS_CONN_ID,
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
        timeout=timedelta(seconds=60*30),
        aws_conn_id=configura_const.AWS_CONN_ID,
        mode="poke"
    )
    
    Computation_file_to_s3 = LocalFilesystemToS3Operator(
        task_id="Computation_file_to_s3",
        aws_conn_id=configura_const.AWS_CONN_ID,
        filename=configura_const.COMPUTE_BOOKS_PATH,
        dest_key=configura_const.COMPUTE_BOOKS_URI,
        replace=True,
    )
    
    add_Computation_step = EmrAddStepsOperator(
        task_id="add_Computation_step",
        steps=configura_const.SPARK_STEPS_COMPUTATION,
        aws_conn_id=configura_const.AWS_CONN_ID,
        job_flow_id="""{{
            task_instance.xcom_pull(
                task_ids='create_spark_emr_cluster', 
                key='return_value'
            ) }}""",
    )
    
    is_Computation_completed = EmrStepSensor(
        task_id="is_Computation_completed",
        timeout=timedelta(seconds=3600),
        poke_interval=timedelta(seconds=10),
        target_states={"COMPLETED"},
        step_id="""{{ 
            task_instance.xcom_pull(
                task_ids='add_Computation_step')[0] 
            }}""",
        job_flow_id="""{{
            task_instance.xcom_pull(
                task_ids='create_spark_emr_cluster', 
                key='return_value'
            ) }}""",
    )
    
    transfer_s3_to_redshift = S3ToRedshiftOperator(
        task_id="transfer_s3_to_redshift",
        redshift_conn_id=configura_const.REDSHIFT_CONN_ID,
        s3_bucket=configura_const.S3_BUCKET_NAME,
        s3_key=configura_const.COMPUTE_BOOKS_URI,
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
    create_aws_s3_bucket >> shell_local_to_s3
    create_aws_s3_bucket >> create_object_in_s3
    create_object_in_s3 >> create_spark_emr_cluster
    is_emr_cluster_created >> add_steps_extraction >> is_extraction_completed
    is_extraction_completed >> transformation_review_step
    is_extraction_completed >> transformation_detail_step
    shell_local_to_s3 >> add_steps_extraction >> is_extraction_completed
    create_aws_s3_bucket >> transformation_review_file_to_s3
    create_aws_s3_bucket >> transformation_detail_file_to_s3
    transformation_review_file_to_s3 >> transformation_review_step >> is_transformation_review_completed
    transformation_detail_file_to_s3 >> transformation_detail_step >> is_transformation_detail_completed
    create_aws_s3_bucket >> Computation_file_to_s3 >> add_Computation_step
    is_transformation_review_completed >> add_Computation_step
    is_transformation_detail_completed >> add_Computation_step
    add_Computation_step >> is_Computation_completed
    is_Computation_completed >> terminate_spark_emr_cluster >> is_emr_cluster_terminated
    is_emr_cluster_terminated >> end_processing_pipeline
    is_Computation_completed >> transfer_s3_to_redshift
    start_processing_pipeline >> create_aws_redshift_cluster >> wait_cluster_available
    wait_cluster_available >> transfer_s3_to_redshift >> end_processing_pipeline