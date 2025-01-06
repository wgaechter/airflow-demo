from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago

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

with DAG(
    'sleeperAPI_MultiTask',
    default_args=default_args,
    description='A simple DAG with Python, SQL Execute, and Postgres operators',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Call Sleeper API to gather all players
    python_task = PythonOperator(
        task_id='python_task',
        python_callable= , # Define a Python function here
        provide_context=True,
    )

    sql_execute_task = SQLExecuteQueryOperator(
        task_id='sql_execute_task',
        sql="SELECT * FROM my_table;",
        conn_id='my_postgres_conn_id',
    )

    postgres_task = PostgresOperator(
        task_id='postgres_task',
        postgres_conn_id='my_postgres_conn_id',
        sql="""
        CREATE TABLE IF NOT EXISTS my_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL
        );
        """,
    )

    python_task >> sql_execute_task >> postgres_task