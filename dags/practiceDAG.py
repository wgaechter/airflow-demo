from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1)
}

dag = DAG(dag_id='practiceDAG', 
          default_args=default_args, 
          schedule_interval='@once',
          catchup=False
    )

start = EmptyOperator(task_id = 'start', dag = dag)
end = EmptyOperator(task_id = 'end', dag = dag)

start >> end