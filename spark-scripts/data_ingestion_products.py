import os
from pathlib import Path
import pyspark
from pyspark.sql.types import StructField, StructType, StringType
from dotenv import load_dotenv

# Load environment variables from .env file
dotenv_path = Path('/opt/app/.env')  # Define the path to the .env file
load_dotenv(dotenv_path=dotenv_path)  # Load the environment variables

# Retrieve PostgreSQL credentials from environment variables
postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')  # PostgreSQL container name
postgres_user = os.getenv('POSTGRES_USER')  # PostgreSQL username
postgres_password = os.getenv('POSTGRES_PASSWORD')  # PostgreSQL password

# Spark configuration for connecting to the Spark cluster
spark_host = "spark://spark-master:7077"  # Define the Spark master URL
sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
    pyspark
    .SparkConf()
    .setAppName("Data Ingestion")  # Set the Spark application name
    .setMaster(spark_host)  # Set the Spark master URL
    .set("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar")  # Include the PostgreSQL driver jar
))
sparkcontext.setLogLevel("WARN")  # Set the Spark log level to warn to minimize unnecessary logs
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())  # Create a Spark session

# PostgreSQL JDBC URL and properties
jdbc_url = f'jdbc:postgresql://{postgres_host}:5432/data_lake'  # Define the JDBC URL for PostgreSQL connection
jdbc_properties = {
    'user': postgres_user,  # PostgreSQL username
    'password': postgres_password,  # PostgreSQL password
    'driver': 'org.postgresql.Driver',  # PostgreSQL JDBC driver
    'stringtype': 'unspecified'  # JDBC property for string type handling
}

# Define the schema for the products dataset
products_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("manufacturer", StringType(), True),
    StructField("base_price", StringType(), True)
])

# Load the products dataset from a CSV file
products_dataset = spark.read.csv("/data/products.csv", header=True, schema=products_schema)

# Write the dataset to PostgreSQL database
try:
    # Overwrite the 'products' table in PostgreSQL with the new data
    products_dataset.write.mode("overwrite").jdbc(jdbc_url, 'public.products', properties=jdbc_properties)
    print("Data successfully saved to PostgreSQL database.")
except Exception as e:
    # Handle any errors that occur during the data writing process
    print("An error occurred while saving data to the PostgreSQL database.")
    print(f"Error: {e}")

# Read the 'products' table from PostgreSQL into a Spark DataFrame
products_df = spark.read.jdbc(jdbc_url, 'public.products', properties=jdbc_properties)

# Display the first 10 rows of the products dataset
print("Products Dataset:")
products_df.show(10)