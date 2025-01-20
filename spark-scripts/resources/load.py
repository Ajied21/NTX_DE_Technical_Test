import os
from pathlib import Path
from dotenv import load_dotenv
import polars as pl
from google.cloud import bigquery

# Google Cloud Configuration
service_account_key = "/scripts/gcp/Service_Account.json"  # Path to the service account JSON key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key  # Set the environment variable for authentication

project_id = "spark-project-416002"  # Google Cloud My Project ID
dataset_id = "ETL_Project"  # BigQuery Dataset My ID

# Load environment variables from a .env file
dotenv_path = Path('/opt/app/.env')  # Path to the .env file
load_dotenv(dotenv_path=dotenv_path)

# Retrieve PostgreSQL connection details from environment variables
postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')  # PostgreSQL container name
postgres_user = os.getenv('POSTGRES_USER')  # PostgreSQL username
postgres_password = os.getenv('POSTGRES_PASSWORD')  # PostgreSQL password

# JDBC URL for connecting to PostgreSQL
jdbc_url = f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:5432/data_transform"

# Define schemas for the product and transaction tables in BigQuery
product_schema = [
    bigquery.SchemaField("product_id", "STRING"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("manufacturer", "STRING"),
    bigquery.SchemaField("base_price", "FLOAT")
]

transaction_schema = [
    bigquery.SchemaField("transaction_id", "STRING"),
    bigquery.SchemaField("customer_id", "STRING"),
    bigquery.SchemaField("product_id", "STRING"),
    bigquery.SchemaField("sale_date", "DATE"),
    bigquery.SchemaField("quantity", "INTEGER"),
    bigquery.SchemaField("total_price", "FLOAT"),
    bigquery.SchemaField("sales_channel", "STRING")
]

merge_schema = [
    bigquery.SchemaField("transaction_id", "STRING"),
    bigquery.SchemaField("customer_id", "STRING"),
    bigquery.SchemaField("product_id", "STRING"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("manufacturer", "STRING"),
    bigquery.SchemaField("sales_channel", "STRING"),
    bigquery.SchemaField("sale_date", "DATE"),
    bigquery.SchemaField("quantity", "INTEGER"),
    bigquery.SchemaField("base_price", "FLOAT"),
    bigquery.SchemaField("total_price", "FLOAT")
]

def load():
    # SQL queries to extract data
    products_query = """
        SELECT DISTINCT
            product_id,
            product_name,
            category,
            manufacturer,
            base_price
        FROM 
            public.transformed_products
    """
    
    transactions_query = """
        SELECT DISTINCT
            transaction_id,
            customer_id,
            product_id,
            sale_date,
            quantity,
            total_price,
            sales_channel 
        FROM 
            public.transformed_transactions
    """

    merge_query = """
        SELECT DISTINCT
            t.transaction_id,
            t.customer_id,
            t.product_id,
            p.product_name,
            p.category,
            p.manufacturer,
            t.sales_channel,
            t.sale_date,
            t.quantity,
            p.base_price,
            t.total_price
        FROM 
            public.transformed_transactions t
        INNER JOIN
            public.transformed_products p
        ON p.product_id = t.product_id
    """
    
    # Read data from PostgreSQL using Polars
    products = pl.read_database(products_query, jdbc_url)
    transactions = pl.read_database(transactions_query, jdbc_url)
    merge = pl.read_database(merge_query, jdbc_url)
    
    # Ensure 'sale_date' column is treated as a string before parsing
    transactions = transactions.with_columns(
        pl.col("sale_date").cast(pl.Utf8)  # Convert sale_date column to string
    )
    
    # Parse 'sale_date' column to Date type
    transactions = transactions.with_columns(
        pl.col("sale_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False)  # Convert sale_date to Date
    )

    # Do the same for the 'merge' DataFrame
    merge = merge.with_columns(
        pl.col("sale_date").cast(pl.Utf8)  # Convert sale_date column to string
    )

    merge = merge.with_columns(
        pl.col("sale_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False)  # Convert sale_date to Date
    )
    
    # Convert Polars DataFrames to Pandas DataFrames for further processing
    products_df = products.to_pandas()
    transactions_df = transactions.to_pandas()
    merge_df = merge.to_pandas()

    # Ensure 'sale_date' in Pandas DataFrame is of type datetime.date
    transactions_df["sale_date"] = transactions_df["sale_date"].dt.date
    merge_df["sale_date"] = merge_df["sale_date"].dt.date

    # Save the DataFrames to temporary Parquet files
    temp_products_file = "/tmp/products.parquet"
    temp_transactions_file = "/tmp/transactions.parquet"
    temp_merge_file = "/tmp/merge.parquet"
    
    products_df.to_parquet(temp_products_file, index=False)
    transactions_df.to_parquet(temp_transactions_file, index=False)
    merge_df.to_parquet(temp_merge_file, index=False)

    # Initialize BigQuery client
    client = bigquery.Client()

    # Load Parquet files to BigQuery tables
    try:
        with open(temp_products_file, "rb") as product_file:
            job_product = client.load_table_from_file(
                product_file, f"{project_id}.{dataset_id}.products", 
                job_config=bigquery.LoadJobConfig(
                    schema=product_schema, 
                    source_format=bigquery.SourceFormat.PARQUET
                )
            )
            job_product.result()  # Wait until the job is complete

        with open(temp_transactions_file, "rb") as transaction_file:
            job_transactions = client.load_table_from_file(
                transaction_file, f"{project_id}.{dataset_id}.transactions", 
                job_config=bigquery.LoadJobConfig(
                    schema=transaction_schema, 
                    source_format=bigquery.SourceFormat.PARQUET
                )
            )
            job_transactions.result()
        
        with open(temp_merge_file, "rb") as merge_file:
            job_transactions = client.load_table_from_file(
                merge_file, f"{project_id}.{dataset_id}.transactions_and_products", 
                job_config=bigquery.LoadJobConfig(
                    schema=merge_schema, 
                    source_format=bigquery.SourceFormat.PARQUET
                )
            )
            job_transactions.result()
        
        print("Data successfully uploaded to BigQuery!")
    except Exception as e:
        print(f"An error occurred while writing to BigQuery: {e}")
    finally:
        # Delete temporary Parquet files
        if os.path.exists(temp_products_file):
            os.remove(temp_products_file)
        if os.path.exists(temp_transactions_file):
            os.remove(temp_transactions_file)

# Entry point for the script
if __name__ == "__main__":

    load()  # Run the load function when the script is executed
