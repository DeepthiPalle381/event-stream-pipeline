from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import pipeline steps from your project
from src.ingest.ingest_events import main as ingest_main
from src.transform.transform_events import main as silver_main
from src.transform.build_gold_tables import main as gold_main

default_args = {
    "owner": "deepthi",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="event_stream_pipeline",
    default_args=default_args,
    description="Daily event stream pipeline: bronze → silver → gold",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["events", "streaming", "data-engineering"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_bronze_events",
        python_callable=ingest_main,
    )

    silver_task = PythonOperator(
        task_id="build_silver_layer",
        python_callable=silver_main,
    )

    gold_task = PythonOperator(
        task_id="build_gold_layer",
        python_callable=gold_main,
    )

    ingest_task >> silver_task >> gold_task
