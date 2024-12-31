from datetime import datetime, timedelta
import requests
import os
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

username = "Mikeym95"

def getUserInfo(username):
    url = "https://api.sleeper.app/v1/user/" + username
    response = requests.get(url)

    rosters = response.json()
    file_path = os.path.join("output/", f"{username}_info.json")

    with open(file_path, "w") as file:
        json.dump(rosters, file, indent=4)

    return rosters

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1)
}

dag = DAG(dag_id='sleeperAPI_userPull', 
          default_args=default_args, 
          schedule_interval='@once',
          catchup=False
    )

start = EmptyOperator(task_id = 'start', dag = dag)
end = EmptyOperator(task_id = 'end', dag = dag)

task = PythonOperator(task_id='getUserInfo', 
                      dag=dag, 
                      python_callable=getUserInfo, 
                      op_kwargs={'username': 'Mikeym95'}
        )

start >> task >> end