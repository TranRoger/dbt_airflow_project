from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_silver_layer',
    default_args=default_args,
    description='Run dbt Silver layer models',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'sqlserver', 'silver'],
)

# Wait for Bronze layer to complete
wait_for_bronze = ExternalTaskSensor(
    task_id='wait_for_bronze',
    external_dag_id='dbt_bronze_layer',
    external_task_id='dbt_test_bronze',
    timeout=600,
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    mode='poke',
    poke_interval=30,
    dag=dag,
)

# Run only Silver layer models
dbt_run_silver = BashOperator(
    task_id='dbt_run_silver',
    bash_command='docker exec dbt_airflow_project-dbt-1 dbt run --models silver',
    dag=dag,
)

# Test Silver layer models
dbt_test_silver = BashOperator(
    task_id='dbt_test_silver',
    bash_command='docker exec dbt_airflow_project-dbt-1 dbt test --models silver',
    dag=dag,
)

# Set task dependencies
wait_for_bronze >> dbt_run_silver >> dbt_test_silver
