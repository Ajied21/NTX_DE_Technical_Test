import os
from pathlib import Path
import pyspark
from pyspark.sql.types import *
from dotenv import load_dotenv

# Load environment variables
dotenv_path = Path('/opt/app/.env')
load_dotenv(dotenv_path=dotenv_path)

postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

# Spark configuration
spark_host = "spark://spark-master:7077"
sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
    pyspark.SparkConf()
    .setAppName("Data Ingestion")
    .setMaster(spark_host)
    .set("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar")
))
sparkcontext.setLogLevel("WARN")
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

jdbc_url = f'jdbc:postgresql://{postgres_host}:5432/data_lake'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

# Load datasets

schema = StructType([
    StructField("fullVisitorId", LongType(), True),
    StructField("channelGrouping", StringType(), True),
    StructField("time", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("totalTransactionRevenue", FloatType(), True),
    StructField("transactions", FloatType(), True),
    StructField("timeOnSite", FloatType(), True),
    StructField("pageviews", FloatType(), True),
    StructField("sessionQualityDim", FloatType(), True),
    StructField("date", IntegerType(), True),
    StructField("visitId", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("productPrice", IntegerType(), True),
    StructField("productSKU", StringType(), True),
    StructField("v2ProductName", StringType(), True),
    StructField("v2ProductCategory", StringType(), True),
    StructField("currencyCode", StringType(), True),
    StructField("pageTitle", StringType(), True),
    StructField("pagePathLevel1", StringType(), True),
    StructField("eCommerceAction_type", IntegerType(), True),
    StructField("eCommerceAction_step", IntegerType(), True),
])

dataset = spark.read.csv("/data/data_transform/e_commerce_session.csv", header=True, schema=schema)

# Write to PostgreSQL
try:
    # Write to PostgreSQL
    dataset.write.mode("overwrite").jdbc(jdbc_url, 'public.e_commerce_session', properties=jdbc_properties)
    print("Data berhasil disimpan ke database PostgreSQL.")
except Exception as e:
    print("Terjadi kesalahan saat menyimpan data ke database PostgreSQL.")
    print(f"Error: {e}")

# Read from PostgreSQL
df = spark.read.jdbc(jdbc_url, 'public.e_commerce_session', properties=jdbc_properties)

# Display data
print("Transactions Dataset:")
df.show(10)