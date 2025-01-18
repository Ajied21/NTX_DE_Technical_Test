import os
from pathlib import Path
from dotenv import load_dotenv
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DateType, FloatType

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
    .setAppName("Data Transform")  # Set the Spark application name
    .setMaster(spark_host)  # Set the Spark master URL
    .set("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar")  # Include the PostgreSQL driver jar
))
sparkcontext.setLogLevel("WARN")  # Set the Spark log level to warn to minimize unnecessary logs
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())  # Create a Spark session

def extract_data_products():
    # extract with query from table products
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
        url=connection_url,
        table=f"({products_query}) AS products",  # Alias query for better compatibility
        properties={
            "user": postgres_user,
            "password": postgres_password,
            "driver": "org.postgresql.Driver"
        }
    )
    return products_df

def extract_data_transactions():
     # extract with query from table transactions
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
        url=connection_url,
        table=f"({transactions_query}) AS transactions",  # Alias query for better compatibility
        properties={
            "user": postgres_user,
            "password": postgres_password,
            "driver": "org.postgresql.Driver"
        }
    )
    return transactions_df

def transform_data():
    
    # Extract data from PostgreSQL using Spark (call extract functions)
    products_df = extract_data_products()  # Get data products
    transactions_df = extract_data_transactions()  # Get data transactions

    print("Transforming data...")

    # Transformation for data products
    transformed_products_df = (
        products_df
        .withColumn("invalid_product_id", F.length(F.col("product_id")) == 13)
        .withColumn("product_id", F.regexp_replace(F.col("product_id"), "MISSING_DATA_", ""))
        .withColumn("product_name", F.regexp_replace(F.col("product_name"), "MISSING_DATA_", ""))
        .withColumn("category", F.regexp_replace(F.col("category"), "MISSING_DATA_", ""))
        .withColumn("manufacturer", F.regexp_replace(F.col("manufacturer"), "MISSING_DATA_", ""))
        .withColumn("base_price", F.regexp_replace(F.col("base_price"), "MISSING_DATA_", ""))
        .withColumn("product_id", F.regexp_extract(F.col("product_id"), r"(PROD-[a-zA-Z0-9]{8})", 1))
        .withColumn("product_name", F.regexp_replace(F.col("product_name"), "-?\d+", ""))      
        .withColumn("product_name", F.regexp_replace(F.col("product_name"), "Books I       ", "Books Item"))
        .withColumn("product_name", F.regexp_replace(F.col("product_name"), "Electroni         ", "Electronics Item"))
        .withColumn("product_name", F.regexp_replace(F.col("product_name"), "Clothing        ", "Clothing Item"))
        .withColumn("product_name", F.regexp_replace(F.col("product_name"), "Beauty & Person               ", "Beauty & Personal Care Item"))
        .withColumn("product_name", F.when(F.col("product_name").isin(
                    ["Home & Kitchen Item ", "Sports & Outdoors Item ", "Books Item ", "Clothing Item ", 
                    "Beauty & Personal Care Item ", "Electronics Item "]),
                                    F.col("product_name")).otherwise("unknown"))
        .withColumn("category", F.regexp_replace(F.col("category"), "Elect     ", "Electronics")) 
        .withColumn("category", F.regexp_replace(F.col("category"), "Beauty & Pe           ", "Beauty & Personal Care"))
        .withColumn("category", F.regexp_replace(F.col("category"), "Sports &        ", "Sports & Outdoors"))
        .withColumn("category", F.regexp_replace(F.col("category"), "Bo  ", "Books"))   
        .withColumn("category", F.regexp_replace(F.col("category"), "Home &        ", "Home & Kitchen"))
        .withColumn("category", F.regexp_replace(F.col("category"), "Clot    ", "Clothing"))  
        .withColumn("category", F.regexp_replace(F.col("category"), "amount", "Home & Kitchen"))
        .withColumn("category", F.regexp_replace(F.col("category"), "service", "Home & Kitchen"))
        .withColumn("category", F.regexp_replace(F.col("category"), "million", "Beauty & Personal Care"))
        .withColumn("category", F.regexp_replace(F.col("category"), "sense", "Clothing"))
        .withColumn("category", F.regexp_replace(F.col("category"), "support", "Beauty & Personal Care"))
        .withColumn("category", F.regexp_replace(F.col("category"), "war", "Beauty & Personal Care")) 
        .withColumn("category", F.regexp_replace(F.col("category"), "you", "Books")) 
        .withColumn("category", F.regexp_replace(F.col("category"), "the", "Sports & Outdoors"))
        .withColumn("category", F.regexp_replace(F.col("category"), "13c4331f", "Clothing"))
        .withColumn("category", F.regexp_replace(F.col("category"), "66aa5854", "Electronics"))
        .withColumn("category", F.regexp_replace(F.col("category"), "customer", "Books"))
        .withColumn("category", F.when(F.col("category").isin(
                    ["Electronics", "Home & Kitchen", "Clothing", "Beauty & Personal Care", 
                    "Books", "Sports & Outdoors"]),
                                F.col("category")).otherwise("unknown"))
        .withColumn("manufacturer", F.regexp_replace(F.col("manufacturer"), "Global      _NOISE_like", "GlobalBrands"))
        .withColumn("manufacturer", F.regexp_replace(F.col("manufacturer"), "Fashio      _NOISE_pretty", "FashionHouse"))
        .withColumn("manufacturer", F.regexp_replace(F.col("manufacturer"), "Book    _NOISE_by", "BookWorld"))
        .withColumn("manufacturer", F.regexp_replace(F.col("manufacturer"), "Tech    ", "TechGiant"))
        .withColumn("manufacturer", F.regexp_replace(F.col("manufacturer"), "Luxur     ", "LuxuryGoods"))
        .withColumn("manufacturer", F.regexp_replace(F.col("manufacturer"), "Book    ", "BookWorld"))
        .withColumn("manufacturer", F.regexp_replace(F.col("manufacturer"), "Global      ", "GlobalBrands"))
        .withColumn("manufacturer", F.regexp_replace(F.col("manufacturer"), "Eco   ", "EcoTech"))
        .withColumn("manufacturer", F.regexp_replace(F.col("manufacturer"), "FashionHouse_NOISE_great", "FashionHouse"))
        .withColumn("manufacturer", F.regexp_replace(F.col("manufacturer"), "FashionHouse_NOISE_reflect", "FashionHouse"))
        .withColumn("manufacturer", F.regexp_replace(F.col("manufacturer"), "GlobalBrands_NOISE_onto", "GlobalBrands"))
        .withColumn("manufacturer", F.regexp_replace(F.col("manufacturer"), "GlobalBrands_NOISE_third", "GlobalBrands"))
        .withColumn("manufacturer", F.regexp_replace(F.col("manufacturer"), "BookWorld_NOISE_white", "BookWorld"))
        .withColumn("manufacturer", F.regexp_replace(F.col("manufacturer"), "BookWorld_NOISE_ok", "BookWorld"))
        .withColumn("manufacturer", F.regexp_replace(F.col("manufacturer"), "BookWorld_NOISE_in", "BookWorld"))
        .withColumn("manufacturer", F.regexp_replace(F.col("manufacturer"), "TechGiant_NOISE_glass", "TechGiant"))
        .withColumn("manufacturer", F.regexp_replace(F.col("manufacturer"), "TechGiant_NOISE_notice", "TechGiant"))
        .withColumn("manufacturer", F.regexp_replace(F.col("manufacturer"), "-?\d+", ""))
        .withColumn("manufacturer", F.when(F.col("manufacturer").isin(
                    ["LuxuryGoods", "FashionHouse", "EcoTech", "SportsPro", 
                    "GlobalBrands", "BookWorld", "TechGiant"]),
                                    F.col("manufacturer")).otherwise("unknown"))
        .withColumn("base_price", F.when(F.col("base_price").rlike("^[0-9]+(\.[0-9]+)?$"), F.col("base_price"))
                .otherwise("0"))
        .withColumn("product_id", F.col("product_id").cast(StringType()))
        .withColumn("product_name", F.col("product_name").cast(StringType())) 
        .withColumn("category", F.col("category").cast(StringType())) 
        .withColumn("manufacturer", F.col("manufacturer").cast(StringType()))
        .withColumn("base_price", F.col("base_price").cast(FloatType())) 
    )

    # Filter rows where the data length does not match
    valid_products_df = transformed_products_df.filter(
        F.col("invalid_product_id")
    )

    # Select the relevant column
    transformed_products_df = valid_products_df.select(
        "product_id", "product_name", "category", "manufacturer", "base_price"
    )

    # Delete rows with NULL values ​​in the relevant columns
    transformed_products_df = transformed_products_df.na.drop(
        subset=["product_id", "product_name", "category", "manufacturer", "base_price"])

    # Displaying transformed data
    transformed_products_df.show()

    # Transformation for data transactions
    transformed_transactions_df = (
        transactions_df
       .withColumn("invalid_transaction_id", F.length(F.col("transaction_id")) == 12)
       .withColumn("invalid_customer_id", F.length(F.col("customer_id")) == 13)
       .withColumn("invalid_product_id", F.length(F.col("product_id")) == 13)
       .withColumn("invalid_sale_date", F.length(F.col("sale_date")) == 10)
       .withColumn("transaction_id", F.regexp_replace(F.col("transaction_id"), "MISSING_DATA_", ""))
       .withColumn("customer_id", F.regexp_replace(F.col("customer_id"), "MISSING_DATA_", ""))
       .withColumn("product_id", F.regexp_replace(F.col("product_id"), "MISSING_DATA_", ""))
       .withColumn("sale_date", F.regexp_replace(F.col("sale_date"), "MISSING_DATA_", ""))
       .withColumn("quantity", F.regexp_replace(F.col("quantity"), "MISSING_DATA_", ""))
       .withColumn("total_price", F.regexp_replace(F.col("total_price"), "MISSING_DATA_", ""))
       .withColumn("sales_channel", F.regexp_replace(F.col("sales_channel"), "MISSING_DATA_", ""))
       .withColumn("transaction_id", F.regexp_extract(F.col("transaction_id"), r"(TRX-[a-zA-Z0-9]{8})", 1))
       .withColumn("customer_id", F.regexp_extract(F.col("customer_id"), r"(CUST-[a-zA-Z0-9]{8})", 1))
       .withColumn("product_id", F.regexp_extract(F.col("product_id"), r"(PROD-[a-zA-Z0-9]{8})", 1))
       .withColumn("quantity", F.regexp_extract(F.col("quantity"), "([1-9])", 1))
       .withColumn("total_price", F.regexp_extract(col("total_price"), r'^\d+(\.\d+)?', 0))
       .withColumn("valid_date", F.col("sale_date").rlike(r"^\d{4}-\d{2}-\d{2}$"))
       .withColumn("sales_channel", F.regexp_replace(F.col("sales_channel"), "In-store", "In-Store"))
       .withColumn("sales_channel", F.when(F.col("sales_channel").contains("_NOISE_"), 
                                    F.split(F.col("sales_channel"), "_NOISE_")[0])
                                    .otherwise(F.col("sales_channel")))
       .withColumn("sales_channel",F.when(F.col("sales_channel").isin(["Online", "Mobile App", "In-Store"]),
                                    F.col("sales_channel")).otherwise("unknown"))
       .withColumn("transaction_id", col("transaction_id").cast(StringType()))
       .withColumn("customer_id", col("customer_id").cast(StringType())) 
       .withColumn("product_id", col("product_id").cast(StringType())) 
       .withColumn("sale_date", col("sale_date").cast(DateType()))
       .withColumn("quantity", col("quantity").cast(IntegerType()))
       .withColumn("total_price", col("total_price").cast(FloatType()))
       .withColumn("sales_channel", col("sales_channel").cast(StringType())) 
    )

    # Filter rows where the data length does not match
    valid_transactions_df = transformed_transactions_df.filter(
        F.col("invalid_transaction_id") |
        F.col("invalid_customer_id") |
        F.col("invalid_product_id") |
        F.col("invalid_sale_date")
    )

    # Filters valid transactions based on the 'valid_date' column and removes that column after the filter is applied.
    df = valid_transactions_df.filter(col("valid_date") == True).drop("valid_date")

    # Select the relevant column
    transformed_transactions_df = df.select(
        "transaction_id", "customer_id", "product_id", "sale_date", "quantity", "total_price", "sales_channel"
    )

    # Delete rows with NULL values ​​in the relevant columns
    transformed_transactions_df = transformed_transactions_df.na.drop(
        subset=["transaction_id", "customer_id", "product_id", "sale_date", "quantity", "total_price", "sales_channel"])

    # Displaying transformed data
    transformed_transactions_df.show()

    # Ingestion or save to postgresql by name database 'data_tranform' and by name table 'transformed_products'
    print("Saving transformed data products to PostgreSQL...")
    transformed_products_df.write.mode("overwrite").jdbc(
        f"jdbc:postgresql://{postgres_host}:5432/data_transform", "public.transformed_products", 
        properties={
            "user": postgres_user,
            "password": postgres_password,
            "driver": "org.postgresql.Driver"
        }
    )

    # Ingestion or save to postgresql by name database 'data_tranform' and by name table 'transformed_transactions'
    print("\nSaving transformed data products to PostgreSQL...")
    transformed_transactions_df.write.mode("overwrite").jdbc(
        f"jdbc:postgresql://{postgres_host}:5432/data_transform", "public.transformed_transactions", 
        properties={
            "user": postgres_user,
            "password": postgres_password,
            "driver": "org.postgresql.Driver"
        }
    )
    print("Transformed data saved successfully!")

# Entry point for the script
if __name__ == "__main__":
    # Call the function to extract and display the data
    transform_data()