# Google Trends Ingestion Service

Architecture and implementation for ingesting data from various apis and storing it on the cloud


## Architecture
Before jumping into the funcitonal and technical details this arch below gives an overview of the landscape. 

![Alt text](resources/DataIngestStoreArch.jpg?raw=true "Google Trends Ingestion Service Architecture")

## Functional Overview
The data ingestion pipeline functions in two stages
- [Stage 1] Data Ingestion Service: A standalone service that fetches the data from the google trends API and pushes it to a kafka topic (Using Kafka acts as an additional backup in case of data corruption in transformaiton layers)

- [Stage 2] Data Store Service: A cluster daemon service that listens to the kafka topic to get the data. A airflow sensor triggers the data store to the cloud (about this in detail in the technical overview). 

## Workflow
![Alt text](resources/workflow_airflow.jpg?raw=true "Workflow of the data ingestion pipelin")
The workflow of the data ingestion pipeline consists of two tasks and one sensor. 
- Task 1: Ingest the data from the google trends api and pushes it to the kafka topic
- Sensor: Sensor to listen to kafka events and trigger the next step
- Task 2: Pushes the received data to the cloud

## Technical Overview
The pipeline is architectured in a way to have a simple and generalized solution. All the modules are loosely coupled. All the modules are built as a standalone service. The repositories of each individual module has been included here. 
### Requirements:
- pytrends==4.8.0 
- kafka-python==1.4.7
- faust==1.10.4
- google-cloud-storage==2.3.0

### Authoring, Scheduling, Monitoring
Apache Airflow is used for the workflow management.
### Development Environment: 
Docker containers were used for development, so a dockerfile and a docker-compose.yaml is included docker-compose is used for orchestration. 

### Logging:
A custom logger has been built which logs the module trace and handles stream and file logging. 
The log trace are created at the head repo of the application
### CI/CD:
Github Actions is used as CI/CD pipeline tool

### [Stage 1] Data Ingestion Service:
This module uses the pytrends==4.8.0 which is a google trends api to fetch the data. Kafka-python (kafka-python==1.4.7) api client is used to connect to kafka cluster. 
The parameters for quering the google trends service (keywords for quering, timeframe to be queried) and the kafka broker parameters (bootstrap servers, port, topic to push the data onto) are configurable at config.py inside this module.  
[Repository for Data Ingestion Service](https://github.com/vivek87799/google_trends_ingest.git)

### [Stage 2] Data Store Service:
This has a standalone daemon service. Faust is used as an alternative to heavy weight Spark and Flink as this stage does not require any heavy data transformation.  
Faust is a light weight stream and event processing framework built with python. It runs multiple agents in parallel and each agent in concurent this potential can be leveraged to handle real time stream processing and batch processing of the data. As all the agents run in parallel it could also use handle light weight data transformation for future enhancements.  
Once the data is received from the kafka topic the agent persists the data to a file which triggers the airflow sensor to initiate the service to store the data to the cloud.  
Please use the dockerfile and config file to set the google cloud connection and the bucket name. 
[Repository for Data Store Service](https://github.com/vivek87799/google_trends_store.git)

### [Data stagging layer] Kafka Cluster:
A kafka cluster with three brokers have been built to stage the data between the ingestion and data store layers

### BigQuery:
Once the data is persisted on the google cloud storage, data flow is triggered to move the data from gcs to bigquery.

### Airflow:
The workflow of the data ingestion pipeline consists of two tasks and one sensor. 
- Task 1: Ingest the data from the google trends api and pushes it to the kafka topic
- Sensor: Sensor to listen to kafka events and trigger the next step
- Task 2: Pushes the received data to the cloud


## To Run

This landscape requires a kafka cluster running on the same network

[Kafka repository](https://github.com/vivek87799/kafka.git)


```bash
docker-compose build
docker-compose up
```

Now you are all set to bring up your workflow using Apache Airflow. The official docker images of apache airflow has been used
```bash
docker-compose build
docker-compose up airflow-init
docker-compose up
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License
[MIT](https://choosealicense.com/licenses/mit/)