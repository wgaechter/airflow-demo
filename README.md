# airflow-demo

Demo project for an Apache Airflow instance in a Docker container.

## Project Overview

This project demonstrates the setup and usage of Apache Airflow within a Docker container. It includes a sample Directed Acyclic Graph (DAG) called `sleeperAPI_MultiTask` to showcase how tasks can be orchestrated and managed using Airflow.

The project utlizes as Data Lake in the form of Amazon S3 Buckets.  Folders of json files are saved and archived to store data for all players, users, leagues, and rosters that have been pulled from the sleeper API.  The data is cleaned and processed into a Postgres Database container for use by the PowerBI Dashboard for each user.

## sleeperAPI_MultiTask DAG

The `sleeperAPI_MultiTask` DAG is designed to demonstrate the following:

- **Task Scheduling**: How tasks can be scheduled to run at specific intervals.
- **Task Dependencies**: How tasks can be dependent on the completion of other tasks.
- **Parallel Execution**: How multiple tasks can be executed in parallel.

### DAG Structure

The `sleeperAPI_MultiTask` DAG consists of the following tasks:

1. **test_s3_connection**: The initial task that tests the connection to S3.
2. **test_postgres_connection**: A task that tests the connection to PostgreSQL.
3. **call_sleeper_api**: A task that calls the Sleeper API.
4. **archive_raw_file**: A task that archives the raw file.
5. **transform_raw_file**: A task that transforms the raw file.
6. **archive_cleaned_file**: A task that archives the cleaned file.
7. **upsert_into_postgres**: The final task that upserts data into PostgreSQL.


## How to Run

1. **Clone the Repository**: Clone this repository to your local machine.
2. **Build the Docker Image**: Navigate to the project directory and build the Docker image using `docker-compose`.
3. **Start Airflow**: Use `docker-compose up` to start the Airflow instance.
4. **Access the Airflow UI**: Open your browser and go to `http://localhost:8080` to access the Airflow web interface.
5. **Trigger the DAG**: In the Airflow UI, trigger the `sleeperAPI_MultiTask` DAG to see it in action.

## Conclusion

This project provides a basic example of how to set up and use Apache Airflow with Docker. The `sleeperAPI_MultiTask` DAG demonstrates task scheduling, dependencies, and parallel execution, which are fundamental concepts in Airflow.

