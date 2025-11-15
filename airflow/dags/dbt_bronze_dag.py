from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "dbt_bronze_layer",
    default_args=default_args,
    description="Run dbt Bronze layer models",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "sqlserver", "bronze"],
)

# Run only Bronze layer models
dbt_run_bronze = BashOperator(
    task_id="dbt_run_bronze",
    bash_command="docker exec dbt_airflow_project-dbt-1 dbt run --models bronze",
    dag=dag,
)

# Test Bronze layer models
dbt_test_bronze = BashOperator(
    task_id="dbt_test_bronze",
    bash_command="docker exec dbt_airflow_project-dbt-1 dbt test --models bronze",
    dag=dag,
)

# Set task dependencies
dbt_run_bronze >> dbt_test_bronze
