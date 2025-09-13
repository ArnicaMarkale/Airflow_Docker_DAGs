from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def print_hello():
    logging.info("Hello from Airflow PythonOperator!")

def print_date():
    logging.info(f"Date: {datetime.now()}")

with DAG(
    "hello_python_airflow",
    default_args=default_args,
    description="PythonOperator example with logging",
    schedule_interval=None,  # Run manually
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    t1 = PythonOperator(
        task_id="say_hello_python",
        python_callable=print_hello
    )

    t2 = PythonOperator(
        task_id="print_date_python",
        python_callable=print_date
    )

    t1 >> t2
