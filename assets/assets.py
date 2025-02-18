import os
import subprocess
import pandas as pd
import numpy as np
import hashlib
from dagster import (
    asset,
    Output,
    Definitions,
    AssetIn,
    get_dagster_logger,
)
from sodapy import Socrata
from resources.minio_io_manager import MinIOIOManager
from resources.mysql_io_manager import MySQLIOManager
from resources.psql_io_manager import PostgreSQLIOManager

logger = get_dagster_logger()
logger.setLevel("INFO")

# Configuration
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "database": "brazillian_ecommerce",
    "user": "admin",
    "password": "admin123",
}

MINIO_CONFIG = {
    "endpoint_url": "localhost:9000",
    "bucket": "warehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
}

PSQL_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "brazillian_ecommerce",
    "user": "admin",
    "password": "admin123",
}
# Get data from Socrata API
user_name = "iambiu6296@gmail.com"
password = "Biugudboi2003@"
api_key = "Gt0bYm8d6gsJqvI1HmUkiQXrK"
start_date = "2016-09-03T07:00:00"
end_date = "2018-11-13T12:00:00"
station_name = "Oak Street Weather Station"
client = Socrata('data.cityofchicago.org', api_key,username=user_name,password=password)

def api_client(station_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    results = client.get("k7hf-8y75", limit=1000000, where=f"station_name = '{station_name}' AND measurement_timestamp BETWEEN '{start_date}' AND '{end_date}'")
    return pd.DataFrame.from_records(results)


# Bronze layer assets
@asset(
    io_manager_key="psql_io_manager",
    required_resource_keys={"psql_io_manager"},
    key_prefix=["brazillian_ecommerce", "bronze"],
    compute_kind="MySQL",
)
def bronze_weather_dataset(context) -> Output[pd.DataFrame]:
    """Extract raw data from open weather data portal API and store in Postgresql"""
    pd_data = api_client(station_name, start_date, end_date)
    logger.info(f"Extracted {len(pd_data)} records from weather dataset")
    return Output(pd_data, metadata={"table": "weather_dataset", "records count": len(pd_data)})

@asset(
    io_manager_key="psql_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["brazillian_ecommerce", "bronze"],
    compute_kind="MySQL",
)
def bronze_olist_customers_dataset(context) -> Output[pd.DataFrame]:
    """Extract raw data from MySQL and store in MinIO"""
    sql_stm = "SELECT * FROM olist_customers_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    logger.info(f"Extracted {len(pd_data)} records from olist_customers_dataset")
    return Output(pd_data, metadata={"table": "olist_customers_dataset", "records count": len(pd_data)})

@asset(
    io_manager_key="psql_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["brazillian_ecommerce", "bronze"],
    compute_kind="MySQL",
)
def bronze_olist_sellers_dataset(context) -> Output[pd.DataFrame]:
    """Extract raw data from MySQL and store in MinIO"""
    sql_stm = "SELECT * FROM olist_sellers_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    logger.info(f"Extracted {len(pd_data)} records from olist_sellers_dataset")
    return Output(pd_data, metadata={"table": "olist_sellers_dataset", "records count": len(pd_data)})

@asset(
    io_manager_key="psql_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["brazillian_ecommerce", "bronze"],
    compute_kind="MySQL",
)
def bronze_olist_products_dataset(context) -> Output[pd.DataFrame]:
    """Extract raw data from MySQL and store in MinIO"""
    sql_stm = "SELECT * FROM olist_products_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    logger.info(f"Extracted {len(pd_data)} records from olist_products_dataset")
    return Output(pd_data, metadata={"table": "olist_products_dataset", "records count": len(pd_data)})

@asset(
    io_manager_key="psql_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["brazillian_ecommerce", "bronze"],
    compute_kind="MySQL",
)
def bronze_olist_orders_dataset(context) -> Output[pd.DataFrame]:
    """Extract raw data from MySQL and store in MinIO"""
    sql_stm = "SELECT * FROM olist_orders_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    logger.info(f"Extracted {len(pd_data)} records from olist_orders_dataset")
    return Output(pd_data, metadata={"table": "olist_orders_dataset", "records count": len(pd_data)})

@asset(
    io_manager_key="psql_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["brazillian_ecommerce", "bronze"],
    compute_kind="MySQL",
)
def bronze_product_category_name_translation(context) -> Output[pd.DataFrame]:
    """Extract raw data from MySQL and store in MinIO"""
    sql_stm = "SELECT * FROM product_category_name_translation"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    logger.info(f"Extracted {len(pd_data)} records from product_category_name_translation")
    return Output(pd_data, metadata={"table": "product_category_name_translation", "records count": len(pd_data)})

@asset(
    io_manager_key="psql_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["brazillian_ecommerce", "bronze"],
    compute_kind="MySQL",
)
def bronze_olist_order_items_dataset(context) -> Output[pd.DataFrame]:
    """Extract raw data from MySQL and store in MinIO"""
    sql_stm = "SELECT * FROM olist_order_items_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    logger.info(f"Extracted {len(pd_data)} records from olist_order_items_dataset")
    return Output(pd_data, metadata={"table": "olist_order_items_dataset", "records count": len(pd_data)})

@asset(
    io_manager_key="psql_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["brazillian_ecommerce", "bronze"],
    compute_kind="MySQL",
)
def bronze_olist_order_payments_dataset(context) -> Output[pd.DataFrame]:
    """Extract raw data from MySQL and store in MinIO"""
    sql_stm = "SELECT * FROM olist_order_payments_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    logger.info(f"Extracted {len(pd_data)} records from olist_order_payments_dataset")
    return Output(pd_data, metadata={"table": "olist_order_payments_dataset", "records count": len(pd_data)})

@asset(
    io_manager_key="psql_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["brazillian_ecommerce", "bronze"],
    compute_kind="MySQL",
)
def bronze_olist_order_reviews_dataset(context) -> Output[pd.DataFrame]:
    """Extract raw data from MySQL and store in MinIO"""
    sql_stm = "SELECT * FROM olist_order_reviews_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    logger.info(f"Extracted {len(pd_data)} records from olist_order_reviews_dataset")
    return Output(pd_data, metadata={"table": "olist_order_reviews_dataset", "records count": len(pd_data)})

# Asset chạy dbt sau khi các asset Bronze hoàn thành
@asset(
    ins={
        "customers": AssetIn(key=["brazillian_ecommerce", "bronze", "bronze_olist_customers_dataset"]),
        "sellers": AssetIn(key=["brazillian_ecommerce", "bronze", "bronze_olist_sellers_dataset"]),
        "products": AssetIn(key=["brazillian_ecommerce", "bronze", "bronze_olist_products_dataset"]),
        "orders": AssetIn(key=["brazillian_ecommerce", "bronze", "bronze_olist_orders_dataset"]),
        "translation": AssetIn(key=["brazillian_ecommerce", "bronze", "bronze_product_category_name_translation"]),
        "order_items": AssetIn(key=["brazillian_ecommerce", "bronze", "bronze_olist_order_items_dataset"]),
        "order_payments": AssetIn(key=["brazillian_ecommerce", "bronze", "bronze_olist_order_payments_dataset"]),
        "order_reviews": AssetIn(key=["brazillian_ecommerce", "bronze", "bronze_olist_order_reviews_dataset"]),
    },
    compute_kind="dbt",
)
def run_dbt(
    context,
    customers,
    sellers,
    products,
    orders,
    translation,
    order_items,
    order_payments,
    order_reviews,
) -> str:
    """
    Asset này sẽ chạy lệnh dbt run trong thư mục DBT_PROJECT_DIR.
    Nó đảm bảo rằng tất cả các asset trong Bronze đã hoàn thành.
    """
    DBT_PROJECT_DIR = "/mnt/d/brazillian_ecommerce_de/brazillian_ecommerce_dbt"
    context.log.info(f"Changing working directory to {DBT_PROJECT_DIR}")
    os.chdir(DBT_PROJECT_DIR)
    
    context.log.info("Running 'dbt run' command...")
    result = subprocess.run(["dbt", "run"], capture_output=True, text=True)
    if result.returncode != 0:
        context.log.error(f"dbt run failed with error: {result.stderr}")
        raise Exception(f"dbt run failed: {result.stderr}")
    
    context.log.info(f"dbt run output: {result.stdout}")
    return "dbt run completed successfully"

# Combine all assets into definitions
defs = Definitions(
    assets=[
        bronze_weather_dataset,
        bronze_olist_customers_dataset,
        bronze_olist_sellers_dataset,
        bronze_olist_products_dataset,
        bronze_olist_orders_dataset,
        bronze_product_category_name_translation,
        bronze_olist_order_items_dataset,
        bronze_olist_order_payments_dataset,
        bronze_olist_order_reviews_dataset,
        run_dbt,
    ],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
    },
)
