from datetime import timedelta
from pathlib import Path
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Main DAG function definition
# This DAG is designed for ingesting transaction data into a staging area using Apache Spark.
# It follows a modular approach to define tasks and execution flow.
@dag(
    description="Dhoifullah Luth Majied",  # Brief description of the DAG
    dag_id=Path(__file__).stem,  # Sets the DAG ID to match the script's filename dynamically
    schedule_interval=None,  # The DAG does not have a predefined schedule and must be triggered manually
    dagrun_timeout=timedelta(minutes=60),  # Specifies the maximum allowed runtime for a single DAG execution
    start_date=days_ago(1),  # Start date for the DAG, set to one day before the current date
    default_args={
        "owner": "Ajied(WhatsApp), Ajied(Email)",  # Identifies the owners of the DAG
    },
    owner_links={
        "Ajied(WhatsApp)": "https://wa.me/+6287787106077",  # Link to contact the owner via WhatsApp
        "Ajied(Email)": "mailto:dhoifullah.luthmajied05@gmail.com",  # Link to contact the owner via email
    },
    tags=["Data Ingestions to Staging Area"],  # Tags for easier organization and filtering of the DAG
)
def spark_Ingestions():

    # Define the tasks and their execution flow within the DAG

    # Starting task: A placeholder task indicating the beginning of the workflow
    start_task = EmptyOperator(task_id="start_task")

    # SparkSubmitOperator task: Executes a Spark job to ingest transaction data
    # This task uses the specified Spark script and PostgreSQL JDBC driver
    Postgresql = SparkSubmitOperator(
        application="/spark-scripts/data_ingestion_transactions.py",  # Path to the Spark application script
        conn_id="spark_main",  # Connection ID for accessing the Spark cluster
        task_id="Transactions",  # Task ID for identification within the DAG
        jars="/spark-scripts/jars/jars_postgresql-42.2.20.jar",  # Path to the PostgreSQL JDBC driver
    )

    # Ending task: A placeholder task indicating the end of the workflow
    end_task = EmptyOperator(task_id="end_task")
    
    # Define the execution flow of tasks
    # The workflow begins with 'start_task', proceeds to the 'Postgresql' task, and concludes with 'end_task'
    start_task >> Postgresql >> end_task

# Instantiate the DAG
spark_Ingestions()