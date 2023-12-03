from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

default_arguments = {
    "owner": "Beryl",
    "email": "berylatieno30@gmail.com",
    "start_date": datetime(2023, 12, 3)
}

def stream_data():
    import json
    import requests

    res = requests.get('https://randomuser.me/api/')
    print(res.json())

with DAG("user_automation",
         default_args=default_arguments,
         schedule_interval= "@daily",
         catchup=False

) as dag:
    
    streaming_task = PythonOperator(
        task_id = "stream_data_from_api",
        python_callable=stream_data
    )