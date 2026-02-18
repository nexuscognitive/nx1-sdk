from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime

# Default arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
}

@dag(
    dag_id="dag_sample_example",
    default_args=default_args,
    description="A simple example DAG for Airflow 3",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example"],
)
def simple_dag():

    @task
    def print_hello():
        print("Hello from Airflow 3!")

    @task
    def print_goodbye():
        print("Goodbye from Airflow 3!")

    # Task dependencies
    hello = print_hello()
    goodbye = print_goodbye()

    hello >> goodbye


# Instantiate the DAG
dag = simple_dag()
