from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
	'owner': 'longdata',
	'retries': 5,
	'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

def input(firstName: str, lastName: str, ti) -> None:
    ti.xcom_push(key='first_name', value=firstName)
    ti.xcom_push(key='last_name', value=lastName)
    
def output(ti) -> None:
    first_name = ti.xcom_pull(task_ids='get_fullName', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_fullName', key='last_name')
    print((first_name, last_name))

def greet(name: str, age: int, major: str, university: str) -> str:
    return f"I'm {name}, {age}, {major} at {university} university"


with DAG(dag_id="practical_airflow_1.4.4",
         default_args=default_args,
         description="""Practical Apache AirFlow, for 
                        InternShip in Viettel Solution""",
         start_date=datetime(2024, 6, 20, 16),
         schedule_interval='0 3 * * Tue-Fri',
         catchup=True) as dag:
    
    first_task = BashOperator(
        task_id="first_task",
        bash_command="echo I am Long Nguyen",
    )
    
    second_task = BashOperator(
        task_id="second_task",
        bash_command="echo I am 21 years old",
    )
    
    third_task = BashOperator(
        task_id="third_task",
        bash_command="echo I am Work at Thang Long University",
    )
    
    temp_task = PythonOperator(
        task_id="temp_task",
        python_callable=greet,
        op_kwargs={
            "name": "Long Nguyen",
            "age": 21,
            "major": "Artificial Intelligence",
            "university": "Thang Long"
        }
    )
    
    four_task = PythonOperator(
        task_id="four_task",
        python_callable=input,
        op_kwargs={
            "firstName": "Long",
            "lastName": "Nguyen"
        }
    )
    
    five_task = PythonOperator(
        task_id="five_task",
        python_callable=output
    )
    
    
    [first_task, second_task, third_task] >> temp_task >> four_task >> five_task
