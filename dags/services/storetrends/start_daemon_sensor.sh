#!/bin/bash
cd /opt/airflow/dags/services/storetrends  &&
faust -A trends_store worker -l info
