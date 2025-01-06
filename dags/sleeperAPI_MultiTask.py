from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
import requests
import json
import boto3

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

#Task Steps
#1. Call Sleeper API to gather all players
#2. Place json data into S3 bucket storage
#3. Load data, remove defenders and lineman, and insert into Postgres table
#4. Place cleaned json into S3 bucket for storage
def call_sleeper_api(**kwargs):
    response = requests.get("https://api.sleeper.app/v1/players/nfl")
    players = response.json()
    with open('/tmp/players.json', 'w') as f:
        json.dump(players, f)
    return '/tmp/players.json'

def upload_to_s3(file_path, bucket_name, s3_key):
    s3 = boto3.client('s3')
    s3.upload_file(file_path, bucket_name, s3_key)

def process_and_load_data(**kwargs):
    with open('/tmp/players.json', 'r') as f:
        players = json.load(f)
    filtered_players = [player for player in players if player['position'] not in ['DEF', 'DL']]
    with open('/tmp/filtered_players.json', 'w') as f:
        json.dump(filtered_players, f)
    # Insert filtered players into Postgres table (pseudo code)
    # insert_into_postgres(filtered_players)
    return '/tmp/filtered_players.json'

def upload_cleaned_data_to_s3(**kwargs):
    upload_to_s3('/tmp/filtered_players.json', 'players', 'filtered_players.json')

with DAG(
    'sleeperAPI_MultiTask',
    default_args=default_args,
    description='A simple DAG with Python, SQL Execute, and Postgres operators',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    call_api_task = PythonOperator(
        task_id='call_sleeper_api',
        python_callable=call_sleeper_api,
        provide_context=True,
    )

    upload_raw_data_task = PythonOperator(
        task_id='upload_raw_data_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={'file_path': '/tmp/players.json', 'bucket_name': 'players', 's3_key': 'players.json'},
    )

    process_data_task = PythonOperator(
        task_id='process_and_load_data',
        python_callable=process_and_load_data,
        provide_context=True,
    )

    upload_cleaned_data_task = PythonOperator(
        task_id='upload_cleaned_data_to_s3',
        python_callable=upload_cleaned_data_to_s3,
        provide_context=True,
    )

    call_api_task >> upload_raw_data_task >> process_data_task >> upload_cleaned_data_task