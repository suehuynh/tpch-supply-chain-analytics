from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from run_pipeline import create_shipping_metrics

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'tpch_pipeline_dag',
    description='Supply Chain DAG',
    default_args=default_args,
    schedule=timedelta(minutes=5),
    start_date=datetime(2026, 3, 25),
    catchup=False,
) as dag:
    @task 
    def create_shipping_metrics_task():
        create_shipping_metrics()
    stop_pipeline = EmptyOperator(task_id='stop_pipeline')

    create_shipping_metrics_task() >> stop_pipeline