from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),  # Ensures DAG starts immediately
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    "demo_dbt_airbnb",
    default_args=default_args,
    description="Run dbt commands for the Airbnb project",
    schedule_interval=None,  # Manually triggered
    catchup=False,
)

# Task 1: Run dbt models
dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="source ~/airflow-venv/bin/activate && cd ~/dbt/demo-dbt-airbnb/dbtairdemo && dbt run --profiles-dir ~/.dbt > ~/airflow/logs/dbt_run.log 2>&1",
    dag=dag,
)

# Task 2: Test dbt models
dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="source ~/airflow-venv/bin/activate && cd ~/dbt/demo-dbt-airbnb/dbtairdemo && dbt test --profiles-dir ~/.dbt > ~/airflow/logs/dbt_test.log 2>&1",
    dag=dag,
)

# Define task dependencies
dbt_run >> dbt_test  # dbt_test runs only if dbt_run succeeds
