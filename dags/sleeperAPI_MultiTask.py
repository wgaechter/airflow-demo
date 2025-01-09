from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta
import requests
import os
import json
import boto3

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

def CallSleeperAPI():
    #Call API for raw json of ALL nfl players
    url = "https://api.sleeper.app/v1/players/nfl"
    response = requests.get(url)
    allPlayers = response.json()
    
    #Place that output file in to the raw json bucket with _new tag.
    s3 = boto3.client('s3')
    s3.put_object(Bucket='bucket s3://fantasy-football-dashboard-prod/players/sleeperAllPlayers/raw/', Key='allPlayers_new.json', Body=json.dumps(allPlayers))


def AllPlayers_raw_archive():
    try:
        s3 = boto3.client('s3')
        bucket = 'fantasy-football-dashboard-prod'
        raw_prefix = 'players/sleeperAllPlayers/raw/'
        raw_archive_prefix = 'players/sleeperAllPlayers/raw/archive/'
        
        # List objects in the raw folder
        objects = s3.list_objects_v2(Bucket=bucket, Prefix=raw_prefix)
        files = [obj['Key'] for obj in objects.get('Contents', [])]
        
        # Check for _current and _new files
        current_file = next((f for f in files if '_current' in f), None)
        new_file = next((f for f in files if '_new' in f), None)
        
        if current_file:
            # Rename _current to _archive+timestamp
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            archive_file = current_file.replace('_current', f'_archive_{timestamp}')
            s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': current_file}, Key=raw_archive_prefix + archive_file.split('/')[-1])
            s3.delete_object(Bucket=bucket, Key=current_file)
        
        # Delete oldest archive if more than 3
        archive_files = sorted([f for f in files if '_archive' in f])
        if len(archive_files) > 3:
            s3.delete_object(Bucket=bucket, Key=archive_files[0])
        
        if new_file:
            # Rename _new to _current
            current_file = new_file.replace('_new', '_current')
            s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': new_file}, Key=current_file)
            s3.delete_object(Bucket=bucket, Key=new_file)
        
        return True
    
    except Exception as e:
        print(f"Error in AllPlayers_raw_archive: {e}")
        return False

def AllPlayers_cleaned_archive():
    try:
        s3 = boto3.client('s3')
        bucket = 'fantasy-football-dashboard-prod'
        cleaned_prefix = 'players/sleeperAllPlayers/cleaned/'
        cleaned_archive_prefix = 'players/sleeperAllPlayers/cleaned/archive/'
        
        # List objects in the cleaned folder
        objects = s3.list_objects_v2(Bucket=bucket, Prefix=cleaned_prefix)
        files = [obj['Key'] for obj in objects.get('Contents', [])]
        
        # Check for _cleaned_current and _cleaned_new files
        current_file = next((f for f in files if '_cleaned_current' in f), None)
        new_file = next((f for f in files if '_cleaned_new' in f), None)
        
        if current_file:
            # Rename _cleaned_current to _cleaned_archive+timestamp
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            archive_file = current_file.replace('_cleaned_current', f'_cleaned_archive_{timestamp}')
            s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': current_file}, Key=cleaned_archive_prefix + archive_file.split('/')[-1])
            s3.delete_object(Bucket=bucket, Key=current_file)
        
        # Delete oldest archive if more than 3
        archive_files = sorted([f for f in files if '_cleaned_archive' in f])
        if len(archive_files) > 3:
            s3.delete_object(Bucket=bucket, Key=archive_files[0])
        
        if new_file:
            # Rename _cleaned_new to _cleaned_current
            current_file = new_file.replace('_cleaned_new', '_cleaned_current')
            s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': new_file}, Key=current_file)
            s3.delete_object(Bucket=bucket, Key=new_file)
        
        return True
    
    except Exception as e:
        print(f"Error in AllPlayers_cleaned_archive: {e}")
        return False

def AllPlayers_cleaning():
    s3 = boto3.client('s3')
    bucket = 'fantasy-football-dashboard-prod'
    raw_prefix = 'players/sleeperAllPlayers/raw/'
    cleaned_prefix = 'players/sleeperAllPlayers/cleaned/'
    
    # Check for _current file in the raw json bucket
    objects = s3.list_objects_v2(Bucket=bucket, Prefix=raw_prefix)
    files = [obj['Key'] for obj in objects.get('Contents', [])]
    current_file = next((f for f in files if '_current' in f), None)
    
    if not current_file:
        return False
    
    # Download the current file
    raw_data = s3.get_object(Bucket=bucket, Key=current_file)['Body'].read().decode('utf-8')
    allPlayers = json.loads(raw_data)

    
    
    # Clean the data (example: remove unusable positions and columns)
    

    # Upload cleaned data to cleaned json bucket
    cleaned_file = current_file.replace('raw', 'cleaned').replace('_current', '_cleaned_new')
    s3.put_object(Bucket=bucket, Key=cleaned_file, Body=json.dumps(cleaned_data))
    
    return True

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1)
}

dag = DAG(dag_id='sleeperAPI_AllPlayers_MultiTask', 
          default_args=default_args, 
          schedule_interval='@hourly',
          catchup=False,
    )

# Task 1: Test connection to AWS S3 buckets, and confirm bucket exists and is operational
test_s3_connection = PythonOperator(
    task_id='test_s3_connection',
    python_callable=lambda: boto3.client('s3').list_buckets(),
    dag=dag
)

# Task 2: Test connection to the Postgres database, and confirm the database exists and is operational
test_postgres_connection = PostgresOperator(
    task_id='test_postgres_connection',
    postgres_conn_id='your_postgres_connection_id',
    sql='SELECT 1;',
    dag=dag
)

# Task 3: Call the Sleeper API to get all players in raw format
# place raw file in s3 bucket s3://fantasy-football-dashboard-prod/players/sleeperAllPlayers/raw/
call_sleeper_api = PythonOperator(
    task_id='call_sleeper_api',
    python_callable=CallSleeperAPI,
    dag=dag
)

# Task 4: S3 operator to rename and archive previous raw file
# delete oldest file in archive folder if more than 3 files
archive_raw_file = PythonOperator(
    task_id='archive_raw_file',
    python_callable=AllPlayers_raw_archive,
    dag=dag
)

# Task 5: Transform raw file into usable FF player data
# remove unusable positions, remove unusable columns
# Copy cleaned json file to cleaned/ folder in s3 bucket, archive previous file in archive/ folder
# remove older archive files if more than 3
transform_raw_file = PythonOperator(
    task_id='transform_raw_file',
    python_callable=AllPlayers_cleaning,
    dag=dag
)

#task 6: Archive Cleaned folders
archive_cleaned_file = PythonOperator(
    task_id='archive_cleaned_file',
    python_callable=AllPlayers_cleaned_archive,
    dag=dag
)

# Task 6: Grab current cleaned file and upsert into Postgres database
upsert_into_postgres = PostgresOperator(
    task_id='upsert_into_postgres',
    postgres_conn_id='your_postgres_connection_id',
    sql='''-- SQL to upsert data into your Postgres database''',
    dag=dag
)

# Define task dependencies
test_s3_connection >> test_postgres_connection >> call_sleeper_api >> archive_raw_file >> transform_raw_file >> archive_cleaned_file >> upsert_into_postgres