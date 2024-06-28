from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator
default_args = {
	'owner': 'longdata',
	'retries': 3,
	'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

def notification() -> None: print("Insert data to postgres db success!")
def check_data() -> None: print("Check data Success!")
def get_pandas() -> None: 
    import pandas
    print(f"Pandas: {pandas.__version__}")

with DAG(dag_id="dag_with_postgres_to_s3_operators__v05",
         default_args=default_args,
         description="""Practical Apache AirFlow, for 
                        InternShip in Viettel Solution""",
         start_date=datetime(2024, 6, 26),
         schedule_interval='0 0 * * *',
         catchup=True
) as dag:
    
    task_1 = PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id="postgres_db_connection",
        database="airflow",
        sql="""
            CREATE TABLE IF NOT EXISTS orders (
                order_id VARCHAR(11),
                date DATE,
                product_name VARCHAR(100),
                quality INTEGER,
                PRIMARY KEY (order_id)
            )
        """)
    
    task_2 = PostgresOperator(
        task_id="create_postgres_table_dag_id",
        postgres_conn_id="postgres_db_connection",
        database="airflow",
        sql="""
            CREATE TABLE IF NOT EXISTS dag_runs (
                dag_id VARCHAR(100),
                date DATE,
                PRIMARY KEY (dag_id)
            )
        """)
    
    task_3 = BashOperator(
        task_id="create_postgres_table_logging",
        bash_command="echo Create Table Success!"
    )
    
    task_7 = PostgresOperator(
        task_id="check_rows_exists_or_not_exists1",
        postgres_conn_id="postgres_db_connection",
        database="airflow",
        sql="""
            DELETE FROM orders 
            WHERE order_id = '123-45-6789'
        """)
    
    task_8 = PostgresOperator(
        task_id="check_rows_exists_or_not_exists2",
        postgres_conn_id="postgres_db_connection",
        database="airflow",
        sql="""
            DELETE FROM dag_runs 
            WHERE dag_id = '{{ dag.dag_id }}'
            AND date = '{{ ds }}';
        """)
    
    task_4 = PostgresOperator(
        task_id="insert_into_postgres_table",
        postgres_conn_id="postgres_db_connection",
        database="airflow",
        sql="""
            INSERT INTO orders(
                order_id,
                date,
                product_name,
                quality
            ) VALUES (
                '123-45-6789',
                '2024-02-01',
                'ChocoPie - Made in VietNam',
                100
            )
        """)
    
    task_5 = PostgresOperator(
        task_id="insert_into_postgres_table_dag_id",
        postgres_conn_id="postgres_db_connection",
        database="airflow",
        sql="""
            INSERT INTO dag_runs(
                dag_id, 
                date
            ) VALUES (
                '{{ dag.dag_id }}',
                '{{ ds }}'
            )
        """)
    
    task_6 = PythonOperator(
        task_id="insert_into_postgres_table_logging",
        python_callable=notification
    )
    
    task_9 = PythonOperator(
        task_id="check_date_logging",
        python_callable=check_data
    )
    
    task_10 = PythonOperator(
        task_id="get_pandas_dependencies",
        python_callable=get_pandas
    )


    [task_1, task_2] >> task_3 >> [task_7, task_8] >> task_9 >> [task_4, task_5] >> task_6
    task_10