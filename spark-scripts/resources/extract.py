import os
from pathlib import Path
from dotenv import load_dotenv
import pyspark
from pyspark.sql import SparkSession

# Load environment variables from .env file
dotenv_path = Path('/opt/app/.env')  # Define the path to the .env file
load_dotenv(dotenv_path=dotenv_path)  # Load the environment variables

# Retrieve PostgreSQL credentials from environment variables
postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')  # PostgreSQL container name
postgres_user = os.getenv('POSTGRES_USER')  # PostgreSQL username
postgres_password = os.getenv('POSTGRES_PASSWORD')  # PostgreSQL password

# Define the JDBC connection URL for PostgreSQL
connection_url = f"jdbc:postgresql://{postgres_host}:5432/data_lake"

# Spark configuration for connecting to the Spark cluster
spark_host = "spark://spark-master:7077"  # Define the Spark master URL
sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
    pyspark
    .SparkConf()
    .setAppName("Data Extract")  # Set the Spark application name
    .setMaster(spark_host)  # Set the Spark master URL
    .set("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar")  # Include the PostgreSQL driver jar
))
sparkcontext.setLogLevel("WARN")  # Set the Spark log level to warn to minimize unnecessary logs
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())  # Create a Spark session

# Function to extract data from the 'products' table in PostgreSQL
def extract_data_products():
    # SQL query to extract product data from PostgreSQL
    products_query = """
                SELECT 
                    product_id,
                    product_name,
                    category,
                    manufacturer,
                    base_price
                FROM 
                    public.products
               """
    # Extract data using Spark JDBC
    print("Extracting data from PostgreSQL (products)...")
    products_df = spark.read.jdbc(
        url=connection_url,  # PostgreSQL JDBC URL
        table=f"({products_query}) AS products",  # Alias query for better compatibility
        properties={
            "user": postgres_user,  # PostgreSQL username
            "password": postgres_password,  # PostgreSQL password
            "driver": "org.postgresql.Driver"  # JDBC driver
        }
    )
    return products_df

# Function to extract data from the 'transactions' table in PostgreSQL
def extract_data_transactions():
    # SQL query to extract transaction data from PostgreSQL
    transactions_query = """
                    SELECT 
                        transaction_id,
                        customer_id,
                        product_id,
                        sale_date,
                        quantity,
                        total_price,
                        sales_channel 
                    FROM 
                        public.transactions
                    """
    # Extract data using Spark JDBC
    print("Extracting data from PostgreSQL (transactions)...")
    transactions_df = spark.read.jdbc(
        url=connection_url,  # PostgreSQL JDBC URL
        table=f"({transactions_query}) AS transactions",  # Alias query for better compatibility
        properties={
            "user": postgres_user,  # PostgreSQL username
            "password": postgres_password,  # PostgreSQL password
            "driver": "org.postgresql.Driver"  # JDBC driver
        }
    )
    return transactions_df

# Function to extract both product and transaction data
def extract_data():
    # Extract product and transaction data using Spark
    products_df = extract_data_products()
    transactions_df = extract_data_transactions()
    
    # Display the extracted product data
    print("Products Data:")
    products_df.show()

    # Display the extracted transaction data
    print("\nTransactions Data:")
    transactions_df.show()

# Entry point for the script
if __name__ == "__main__":
    # Call the function to extract and display the data
    extract_data()