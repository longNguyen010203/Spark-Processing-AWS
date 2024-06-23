from pathlib import Path
from datetime import datetime, timedelta
from tasks.extract_data_from_csv import extract_data
from tasks.extract_data_from_csv import get_shape

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



default_args = {
	'owner': 'longdata',
	'retries': 5,
	'retry_delay': timedelta(minutes=10),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}


with DAG(dag_id="dag_csv_to_postgres",
         default_args=default_args,
         description="""
            Extract data from csv file 
            and load to postgres database
         """,
         start_date=datetime(2024, 6, 23),
         schedule_interval='0 0 * * *') as dag:
    
    CSV_PATH = Path(__file__).joinpath("..", "data").resolve()
    
    extract_data_from_csv = PythonOperator(
        task_id="extract_data_from_csv",
        python_callable=extract_data(CSV_PATH),
        dag=dag
    )
    
    get_shape_from_data = PythonOperator(
        task_ids="get_shape_from_data",
        python_callable=get_shape,
        dag=dag
    )
    
    extract_data_from_csv >> get_shape_from_data