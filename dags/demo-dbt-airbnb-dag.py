from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define local path for dbt project
DBT_LOCAL_PATH = "/home/ec2-user/demo-dbt-airbnb/dbtairdemo/"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1
}

with DAG(
    'demo-dbt-airbnb-dag',
    default_args=default_args,
    schedule_interval='@daily',  # Adjust as needed
    catchup=False
) as dag:

    # Task: Run dbt commands
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command=f'cd {DBT_LOCAL_PATH} && dbt run'
    )
