FROM ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.3.0-python3.8}

# To be moved to .env
ENV GCP_GCS_BUCKET="trends-data01"
ENV DESTINATION_BLOB_NAME="trends_data"
ENV AIRFLOW_UID=1001
ENV FILENAME_FROM_STREAM="data_trends.csv"

USER airflow

WORKDIR /opt/airflow
COPY requirements.txt requirements.txt 
RUN pip install -r requirements.txt
COPY . /opt/airflow

# cmd to start the daemon service
# If your container has started as root please attach to the terminal and start the daemon job manually
CMD ["start_daemon_sensor.sh"]
