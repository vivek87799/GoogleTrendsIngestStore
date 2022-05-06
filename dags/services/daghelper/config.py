class Fields:
    WAITING_FOR_FILE = "waiting_for_trigger"
    CONN_ID = "fs_default"
    GCP_GCS_BUCKET = "GCP_GCS_BUCKET"
    
    GCP_DESTINATION_BLOB_NAME = "GCP_DESTINATION_BLOB_NAME"
    DESTINATION_BLOB_NAME = "DESTINATION_BLOB_NAME"
    TAGS = "Google Trends ingest and store"

class Parameters: 
    DEFAULT_BUCKET_NAME = "trends-data01"
    DEFAULT_GCP_DESTINATION_BLOB_NAME = "trends_data"
    DEFAULT_DESTINATION_BLOB_NAME= "trends_data"
    SERVICE_ACC_KEY_PATH = "dags/banded-splicer-349115-ad22363c394a.json"
