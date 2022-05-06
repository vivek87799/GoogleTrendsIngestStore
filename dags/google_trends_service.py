# Logical Implementaion
# Create two tasks 
# 1) One to ingest the data from the google trends service and push to kafka for backup persistance
# 2) Another to stream the data from the kafka topic and push it to the google cloud


import os

import pathlib
import pendulum
from typing import List, Dict, Any

from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
from airflow.hooks.filesystem import FSHook
from airflow.sensors.filesystem import FileSensor

from google.cloud import storage

from services.ingesttrends import trends_service
# Importing the custom stream and file logger
from services.ingesttrends.helper_functions import log, logger
from services.daghelper.config import Fields, Parameters


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=[Fields.TAGS],
)
def google_trends_ingest_store()-> None:
    """
    This is a data ingestion and storage pipeline for the data fetched from the google trends service
    """

    FILENAME_FROM_STREAM = os.environ.get("FILENAME_FROM_STREAM", "data_trends.csv") 
    
    @task()
    @log
    def ingest() -> None:
        """
        #### Ingest task
        A simple ingest task to fetch the data from the google trends service and push it into temporary persistant layer (kafka)
        .
        """

        # The default configuration gets 3 years of data for 5 different keywords 
        # The duration keywords, host language, and time zone are customizable 
        # with the config file attached to the service module. This service module 
        # fetches the data and pushes it to kafka topic
        data_ingest_service = trends_service.GoogleTrendsService()
        data_ingested = data_ingest_service.run_manager()
        logger.debug(data_ingested)
        
    
    # File sensor that waits for the file to be created from the kafka stream
    sensors = FileSensor(
        task_id=Fields.WAITING_FOR_FILE,
        filepath=FILENAME_FROM_STREAM,
        fs_conn_id=Fields.CONN_ID, 
        poke_interval=5
    )

    @task()
    @log
    def store() -> None:
        """
        #### Store task
        A simple store task to persist the data onto google cloud from the kafka topic
        """
        
        # Read the file from the sensor hook
        file_dir = FSHook(conn_id=Fields.CONN_ID).get_path()
        file_path = pathlib.Path(f"{file_dir}/{FILENAME_FROM_STREAM}")
        logger.debug(f"{file_path}")
        
        logger.debug("Retriving GCP_GCS_BUCKET from env")
        BUCKET_NAME = os.environ.get(Fields.GCP_GCS_BUCKET, Parameters.DEFAULT_BUCKET_NAME)
        DESTINATION_BLOB_NAME = os.environ.get(Fields.DESTINATION_BLOB_NAME, Parameters.DEFAULT_DESTINATION_BLOB_NAME)
        
        # TO be secured
        storage_client = storage.Client.from_service_account_json(Parameters.SERVICE_ACC_KEY_PATH)
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(DESTINATION_BLOB_NAME)

        blob.upload_from_filename(file_path)
        logger.debug("Retrived data successfully pushed to google cloud storage")
        
        # Delete the file
        file_path.unlink()
     
    ingest() >> sensors >> store()


google_trends_ingest_store_dag = google_trends_ingest_store()


