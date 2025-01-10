from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta
import requests
import os
import json
import boto3

AWS_S3_CONN_ID = 's3_conn'

bucket = 'fantasy-football-dashboard-prod'
raw_prefix = 'players/sleeperAllPlayers/raw/'
raw_archived_prefix = 'players/sleeperAllPlayers/raw/archive/'
cleaned_prefix = 'players/sleeperAllPlayers/cleaned/'
cleaned_archived_prefix = 'players/sleeperAllPlayers/cleaned/archive/'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

def testS3Conn():
    try:
        hook = S3Hook(aws_conn_id=AWS_S3_CONN_ID)
        check = hook.check_for_bucket(bucket)
        if not check:
            raise Exception(f"Bucket {bucket} does not exist or is not accessible.")
        return True
    except Exception as e:
        print(e)
        return False


def CallSleeperAPI():
    try:
        # Call API for raw json of ALL nfl players
        url = "https://api.sleeper.app/v1/players/nfl"
        response = requests.get(url)
        allPlayers = response.json()
        
        # Place that output file in to the raw json bucket with _new tag.
        hook = S3Hook(aws_conn_id=AWS_S3_CONN_ID)
        hook.load_string(json.dumps(allPlayers), key=raw_prefix + 'allPlayers_raw_new.json', bucket_name=bucket)    
        return True
    
    except Exception as e:
        print(f"Error in CallSleeperAPI: {e}")
        return False

def AllPlayers_raw_archive():
    try:
        hook = S3Hook(aws_conn_id=AWS_S3_CONN_ID)
        objects = hook.list_keys(bucket_name=bucket, prefix=raw_prefix)
        archives = hook.list_keys(bucket_name=bucket, prefix=raw_archived_prefix)
        current_file = next((f for f in objects if '_raw_current' in f), None)
        
        if not current_file:
            raise Exception("No current file found")
        
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        archive_file = current_file.replace('_raw_current', f'_raw_archive_{timestamp}')
        hook.copy_object(source_bucket_key=current_file, dest_bucket_key=raw_archived_prefix + archive_file.split('/')[-1], source_bucket_name=bucket, dest_bucket_name=bucket)
        hook.delete_objects(bucket=bucket, keys=[current_file])
        
        archive_files = sorted([f for f in archives if '_raw_archive' in f])
        if len(archive_files) > 1:
            hook.delete_objects(bucket=bucket, keys=[archive_files[0]])
            
        new_file = raw_prefix + 'allPlayers_raw_new.json'
        replacement_file = new_file.replace('_raw_new', '_raw_current')
        hook.copy_object(source_bucket_key=new_file, dest_bucket_key=replacement_file, source_bucket_name=bucket, dest_bucket_name=bucket)
        hook.delete_objects(bucket=bucket, keys=[new_file])
        return True
    
    except Exception as e:
        print(f"Error in AllPlayers_raw_archive: {e}")
        return False

def AllPlayers_cleaned_archive():
    try:
        s3 = boto3.client('s3_conn')
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
        if len(archive_files) > 1:
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

def ScrubUnneededPositions(players):
    # Remove positions that are not used in fantasy football
    # AI Nonsese lmao
    
    return players

def ScrubInactivePlayers(players):
    # Remove players that are not active
    return [player for player in players if player['status'] == 'Active']

def AllPlayers_cleaning():
    hook = S3Hook(aws_conn_id=AWS_S3_CONN_ID)
    
    # Check for _current file in the raw json bucket
    objects = hook.list_keys(bucket_name=bucket, prefix=raw_prefix)
    current_file = next((f for f in objects if '_raw_current' in f), None)
    #Catch if no current file is found
    if not current_file:
        return False

    # Download the current file
    raw_data = hook.read_key(key=current_file, bucket_name=bucket)
    allPlayers = json.loads(raw_data)

    raw_size = len(allPlayers)

    # Clean the data (example: remove unusable positions and columns)
    releventPlayers = ScrubUnneededPositions(allPlayers)
    remaining = raw_size - len(releventPlayers)
    print(f"Removed {remaining} players from raw data")

    cleanedPlayers = ScrubInactivePlayers(releventPlayers)
    removed = len(releventPlayers) - len(cleanedPlayers)
    print(f"Removed {removed} inactive players from raw data")

    # Upload cleaned data to cleaned json bucket
    cleaned_file = current_file.replace('raw', 'cleaned').replace('_raw_current', '_cleaned_new')
    hook.load_string(json.dumps(cleanedPlayers), key=cleaned_file, bucket_name=bucket)
    
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
    python_callable=testS3Conn,
    dag=dag
)

# Task 2: Test connection to the Postgres database, and confirm the database exists and is operational


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
test_s3_connection >> call_sleeper_api >> archive_raw_file >> transform_raw_file >> archive_cleaned_file >> upsert_into_postgres

if __name__ == "__main__":
    dag.test()