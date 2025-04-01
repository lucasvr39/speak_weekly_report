import snowflake.connector
from dotenv import load_dotenv
import os
from os.path import join, dirname
from datetime import date as dt
from azure.storage.blob import BlobClient
import io
import pandas as pd
import sys
import logging
from datetime import datetime


# Configure logging
if not os.path.exists("./logs"):
    os.makedirs("./logs")

log_filename = f"logs/{datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_filename), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def csv_to_memory_buffer(df: pd.DataFrame) -> io.BytesIO:
    csv_buffer = io.BytesIO()
    logger.info(f"Preview of data to be exported:\n{df.head()}")
    if not df.empty:
        df.to_csv(csv_buffer, index=False, encoding="utf-8")
        csv_buffer.seek(0)
        logger.info(
            f"Successfully converted DataFrame with {len(df)} rows to CSV buffer"
        )
        return csv_buffer
    else:
        logger.warning("The dataframe is empty.")
        return csv_buffer


def upload_to_blob(csv_buffer: io.BytesIO, sas_url: str) -> None:
    try:
        client = BlobClient.from_blob_url(sas_url)
        client.upload_blob(csv_buffer)
        logger.info("Successfully uploaded data to Azure Blob Storage")
    except Exception as e:
        logger.error(f"Failed to upload to blob storage: {str(e)}")
        raise


def get_config():
    dotenv_path = join(dirname(__file__), ".env")
    config_loaded = load_dotenv(dotenv_path)
    if config_loaded:
        logger.info("Configuration loaded successfully")
    else:
        logger.error("Failed to load configuration")
    return config_loaded


def connect_to_snowflake():
    if not get_config():
        logger.error("Failed to load configuration")
        return None

    try:
        conn = snowflake.connector.connect(
            user=os.environ.get("LOGIN"),
            password=os.environ.get("PASSWORD"),
            account=os.environ.get("ACCOUNT"),
            warehouse=os.environ.get("WAREHOUSE"),
            database=os.environ.get("DATABASE"),
            schema=os.environ.get("SCHEMA"),
            role=os.environ.get("ROLE"),
            telemetry=False,
            verbose=True,
        )
        logger.info("Successfully connected to Snowflake")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {str(e)}")
        sys.exit(1)


def execute_query(conn, query):
    try:
        cursor = conn.cursor()
        logger.info("Executing Snowflake query")
        cursor.execute(query)
        query_df = cursor.fetch_pandas_all()
        logger.info(f"Query executed successfully, retrieved {len(query_df)} rows")
        return query_df
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        return None
    finally:
        cursor.close()


if __name__ == "__main__":
    logger.info("Starting process")

    try:
        conn = connect_to_snowflake()
        # Hiding the query lol
        q = os.environ.get("QUERY")
        base_ef_df = execute_query(conn, q)

        today_dt = dt.today().strftime("%Y%m%d")
        base_ef_name = f"base_EF_{today_dt}.csv"
        sas_url = (
            f"{os.environ.get('BASE_URL')}{base_ef_name}?{os.environ.get('SAS_TOKEN')}"
        )

        buffer = csv_to_memory_buffer(base_ef_df)
        upload_to_blob(buffer, sas_url)
        logger.info("Data retrieve and upload process completed successfully")
    except Exception as e:
        logger.error(f"Data retrieve and upload process failed: {str(e)}")
        sys.exit(1)
