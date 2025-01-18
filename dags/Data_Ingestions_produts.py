from datetime import timedelta
from pathlib import Path
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Main DAG function definition
# This DAG is designed to perform data ingestion tasks using Apache Spark. 
# It is tailored for ingesting data into a staging area with a modular and reusable setup.
@dag(
    description="Dhoifullah Luth Majied",  # A brief description of the DAG
    dag_id=Path(__file__).stem,  # Dynamically sets the DAG ID to match the script's filename
    schedule_interval=None,  # No specific schedule; manual trigger required
    dagrun_timeout=timedelta(minutes=60),  # Maximum allowed runtime for a single DAG run
    start_date=days_ago(1),  # Start date for the DAG, set to one day before the current date
    default_args={
        "owner": "Ajied(WhatsApp), Ajied(Email)",  # Owners responsible for managing this DAG
    },
    owner_links={
        "Ajied(WhatsApp)": "https://wa.me/+6287787106077",  # Link to contact the owner via WhatsApp
        "Ajied(Email)": "mailto:dhoifullah.luthmajied05@gmail.com",  # Link to contact the owner via email
    },
    tags=["Data Ingestions to Staging Area"],  # Tags for easier classification and filtering
)
def spark_Ingestions():

    # Define the tasks and the execution flow of the DAG

    # Starting task: A placeholder task to indicate the start of the workflow
    start_task = EmptyOperator(task_id="start_task")

    # SparkSubmitOperator task: Executes a Spark job for data ingestion
    # This task is configured to run a specific Spark application for processing product data
    Postgresql = SparkSubmitOperator(
        application="/spark-scripts/data_ingestion_products.py",  # Path to the Spark application script
        conn_id="spark_main",  # Connection ID to the Spark cluster
        task_id="Products",  # Task ID for identification within the DAG
        jars="/spark-scripts/jars/jars_postgresql-42.2.20.jar",  # Path to the PostgreSQL JDBC driver for Spark
    )

    # Ending task: A placeholder task to indicate the end of the workflow
    end_task = EmptyOperator(task_id="end_task")
    
    # Define the execution flow of tasks
    # The workflow starts with the 'start_task', proceeds to the 'Postgresql' task, and ends with the 'end_task'
    start_task >> Postgresql >> end_task

# Instantiate the DAG
spark_Ingestions()