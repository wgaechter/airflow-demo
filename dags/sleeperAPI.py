from datetime import datetime, timedelta
import requests
import os
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook


# Define the function to pull user information from the Sleeper API
# Username will later be updated to come from the API call from powerBI on login
username = "Mikeym95"
def GetUserInfo(username):
    url = "https://api.sleeper.app/v1/user/" + username
    response = requests.get(url)

    rosters = response.json()

    return rosters

def GetAllPlayers():
    url = "https://api.sleeper.app/v1/players/nfl"
    response = requests.get(url)
    allPlayers = response.json()

    return allPlayers

def CreateAllPlayersTable(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS allPlayers (
            sleeper_player_id VARCHAR PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            age INT,
            years_exp INT,
            position VARCHAR,
            fantasy_positions text[],
            team VARCHAR,
            team_abbr VARCHAR,
            depth_chart_position VARCHAR,
            depth_chart_order INT,
            practice_description VARCHAR,
            status VARCHAR,
            injury_status VARCHAR,
            injury_body_part VARCHAR,
            injury_start_date DATE
        )
    """)

    return True

# Define the function to upsert the allPlayers table records in the database
def UpsertAllPlayersTable():
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_ffdata', schema='airflow')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        # Create the allPlayers table if it does not exist
        CreateAllPlayersTable(cursor)

        # Get all players from the Sleeper API
        allPlayers = GetAllPlayers()

        for player in allPlayers.values():
            print(player)
            cursor.execute(
            """
            INSERT INTO allPlayers (sleeper_player_id, first_name, last_name, age, years_exp, position, fantasy_positions, team, team_abbr, depth_chart_position, depth_chart_order, practice_description, status, injury_status, injury_body_part, injury_start_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
            ON CONFLICT (sleeper_player_id) DO UPDATE
            SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                age = EXCLUDED.age,
                years_exp = EXCLUDED.years_exp,
                position = EXCLUDED.position,
                fantasy_positions = EXCLUDED.fantasy_positions,
                team = EXCLUDED.team,
                team_abbr = EXCLUDED.team_abbr,
                depth_chart_position = EXCLUDED.depth_chart_position,
                depth_chart_order = EXCLUDED.depth_chart_order,
                practice_description = EXCLUDED.practice_description,
                status = EXCLUDED.status,
                injury_status = EXCLUDED.injury_status,
                injury_body_part = EXCLUDED.injury_body_part,
                injury_start_date = EXCLUDED.injury_start_date
            """,
            (
                player.get('player_id'),
                player.get('first_name'),
                player.get('last_name'),
                player.get('age'),
                player.get('years_exp'),
                player.get('position'),
                player.get('fantasy_positions'),
                player.get('team'),
                player.get('team_abbr'),
                player.get('depth_chart_position'),
                player.get('depth_chart_order'),
                player.get('practice_description'),
                player.get('status'),
                player.get('injury_status'),
                player.get('injury_body_part'),
                player.get('injury_start_date')
            )
        )
            
            print(f"Upserted player {player.get('first_name')} {player.get('last_name')}")
        
        connection.commit()
        connection.close()
        return True
    
    except Exception as e:
        print(e)
        return str(e)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1)
}

dag = DAG(dag_id='sleeperAPI_UpsertAllPlayersTable', 
          default_args=default_args, 
          schedule_interval='@hourly',
          catchup=False,
    )

UpsertAllPlayersTable = PythonOperator(task_id='UpsertAllPlayersTable', 
                      dag=dag, 
                      python_callable=UpsertAllPlayersTable
        )

UpsertAllPlayersTable
