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

    response = requests.get('https://randomuser.me/api/')
    if response.status_code == 200:
        data = response.json()
        data = data['results'][0]
        print(json.dumps(data, indent=3))
    else:
        print(f"Error: {response.status_code} - {response.text}")

with DAG("user_automation",
         default_args=default_arguments,
         schedule = "@daily",
         catchup=False

) as dag:
    
    streaming_task = PythonOperator(
        task_id = "stream_data_from_api",
        python_callable=stream_data
    )

stream_data()