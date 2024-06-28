from datetime import datetime, timedelta

from airflow.decorators import dag, task



default_args = {
	'owner': 'longdata',
	'retries': 5,
	'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

@dag(dag_id="youtube_etl_pipeline_v1.0.1",
     default_args=default_args,
     description="""Practical Apache AirFlow, for 
                    InternShip in Viettel Solution""",
     start_date=datetime(2024, 6, 25, 16),
     schedule_interval="@daily")
def aws_etl_pipeline():
    
    @task
    def get_student_code() -> str:
        return "A44306"
    
    @task
    def get_major() -> str:
        return "Artificial Intelligence"
    
    @task
    def greet(student_code: str, major: str, fullName: dict[str, str]) -> str:
        print(f"{student_code} - {fullName["firstName"]} {fullName["lastName"]} - {major}")
        
    @task(multiple_outputs=True)
    def get_fullName() -> dict[str, str]:
        return {
            "firstName": "Long",
            "lastName": "Nguyen"
        }
        
        
    student_code = get_student_code()
    major = get_major()
    fullName = get_fullName()
    greet(student_code=student_code, major=major, fullName=fullName)
    
    
greet_dag = aws_etl_pipeline()