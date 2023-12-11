from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

default_arguments = {
    "owner": "Beryl",
    "email": "berylatieno30@gmail.com",
    "start_date": datetime(2023, 12, 3)
}

#get data from random user generator API

def get_data():
    import requests

    response = requests.get('https://randomuser.me/api/')
    if response.status_code == 200:
        response = response.json()
        response = response['results'][0]

        return response
    else:
        print(f"Error: {response.status_code} - {response.text}")

#Formating the API response into a dictionary

def format_data(response):
    data = {}
    data['first_name'] = response['name']['first']
    data['last_name'] = response['name']['last']
    data['gender'] = response['gender']
    data['address'] = (str(response['location']['street']['number']) + " " 
                       + str(response['location']['street']['name']) + " " 
                       + str(response['location']['city']) + " " 
                       + str(response['location']['state']) + " " 
                       + str(response['location']['country']))
    data['post_code'] = response['location']['postcode']
    data['email'] = response['email']
    data['username'] = response['login']['username']
    data['age'] = response['dob']['age']
    data['dob'] = response['dob']['date']
    data['registration_date'] = response['registered']['date']
    data['phone'] = response['phone']
    data['picture'] = response['picture']['medium']

    return data


#Streaming the data
def stream_data():
    import json
    res = get_data()
    res = format_data(res)
    print(json.dumps(res, indent=3))
    

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