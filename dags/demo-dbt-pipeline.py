from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define the S3 bucket and local path
S3_BUCKET = "demo-dbt-airbnb"  # Change to your actual bucket name
DBT_S3_PATH = f"s3://{S3_BUCKET}/dbt/"
DBT_LOCAL_PATH = "/usr/local/airflow/dbt_project/"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1
}

with DAG(
    'demo-dbt-pipeline',
    default_args=default_args,
    schedule_interval='@daily',  # Adjust as needed
    catchup=False
) as dag:

    # Task 1: Download the dbt project from S3 using AWS CLI
    download_dbt_project = BashOperator(
        task_id='download_dbt_project',
        bash_command=f'aws s3 sync {DBT_S3_PATH} {DBT_LOCAL_PATH}'
    )

    # Task 2: Run dbt commands
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command=f'cd {DBT_LOCAL_PATH} && dbt run'
    )

    # Define task dependencies
    download_dbt_project >> run_dbt
