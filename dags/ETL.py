from datetime import timedelta
from pathlib import Path
from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Main DAG function definition
# This DAG performs a complete ETL (Extract, Transform, Load) process using Apache Spark and Bash scripts.
@dag(
    description="Dhoifullah Luth Majied",  # Brief description of the DAG
    dag_id=Path(__file__).stem,  # Dynamically sets the DAG ID to match the script's filename
    schedule_interval=None,  # No predefined schedule; the DAG needs to be triggered manually
    dagrun_timeout=timedelta(minutes=60),  # Maximum allowed runtime for a single DAG run
    start_date=days_ago(1),  # Start date for the DAG, set to one day before the current date
    default_args={
        "owner": "Ajied(WhatsApp), Ajied(Email)",  # Identifies the owners of the DAG
    },
    owner_links={
        "Ajied(WhatsApp)": "https://wa.me/+6287787106077",  # Link to contact the owner via WhatsApp
        "Ajied(Email)": "mailto:dhoifullah.luthmajied05@gmail.com",  # Link to contact the owner via email
    },
    tags=["ETL"],  # Tags to categorize the DAG as part of ETL processes
)
def extract_transform_load():

    # Define the tasks and their execution flow within the DAG

    # Starting task: A placeholder task to indicate the beginning of the workflow
    start_task = EmptyOperator(task_id="start_task")
    
    # Extract task: Executes a Spark job to extract data
    # This task processes data from the source and prepares it for transformation
    extract = SparkSubmitOperator(
        application='/spark-scripts/resources/extract.py',  # Path to the Spark script for data extraction
        conn_id="spark_main",  # Connection ID for accessing the Spark cluster
        task_id="extract_data",  # Task ID for identification within the DAG
        execution_timeout=timedelta(seconds=60),  # Maximum runtime for the task
        jars="/spark-scripts/jars/jars_postgresql-42.2.20.jar",  # Path to the PostgreSQL JDBC driver for Spark
    )

    # Transform task: Executes a Spark job to transform data
    # This task applies business logic and prepares the data for loading
    transform = SparkSubmitOperator(
        application='/spark-scripts/resources/transform.py',  # Path to the Spark script for data transformation
        conn_id="spark_main",  # Connection ID for accessing the Spark cluster
        task_id="transform_data",  # Task ID for identification within the DAG
        jars="/spark-scripts/jars/jars_postgresql-42.2.20.jar",  # Path to the PostgreSQL JDBC driver for Spark
    )

    # Load task: Executes a Bash script to load the transformed data
    # This task integrates the processed data into the target database
    load = BashOperator(
        task_id='load_data',  # Task ID for identification within the DAG
        bash_command='python /spark-scripts/resources/load.py'  # Command to execute the Python load script
    )

    # Ending task: A placeholder task to indicate the end of the workflow
    end_task = EmptyOperator(task_id="end_task")
    
    # Define the execution flow of tasks
    # The workflow starts with 'start_task', proceeds to 'extract', 'transform', and 'load', and concludes with 'end_task'
    start_task >> extract >> transform >> load >> end_task

# Instantiate the DAG
extract_transform_load()