#!/bin/bash
cd /opt/airflow/dags/services/storetrends  &&
exec "nohup faust -A trends_store worker -l info &"
