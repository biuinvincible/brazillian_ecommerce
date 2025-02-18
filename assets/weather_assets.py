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
    resource
)
from sodapy import Socrata
from json import dumps, loads
#from resources.minio_io_manager import MinIOIOManager
#from resources.mysql_io_manager import MySQLIOManager
#from resources.psql_io_manager import PostgreSQLIOManager

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

# Bronze layer assets
def api_client(station_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    results = client.get("k7hf-8y75", limit=1000000, where=f"station_name = '{station_name}' AND measurement_timestamp BETWEEN '{start_date}' AND '{end_date}'")
    return pd.DataFrame.from_records(results)

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