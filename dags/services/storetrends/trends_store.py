#!/usr/bin/env python3

# Logical implementaion
# 1) create a payload
#
# 2) request the keywords
#    

import os
import faust
import json
import csv

from typing import List

from config import KafkaConfig, Parameters, Fields
from helper_functions import log, logger


class TrendsData(faust.Record, serializer=Fields.JSON):
    data = List[str]


app = faust.App(
    KafkaConfig.GROUP_ID,
    broker=KafkaConfig.KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=Fields.RAW

)

data_topic = app.topic(KafkaConfig.TOPIC)

@log
def persist_data(data):
    logger.debug("data passed to sink")
    keys = data[0].keys()
    with open("data_trends.csv", "w+", encoding="utf-8", newline="") as out_file:
    	dict_writer = csv.DictWriter(out_file, fieldnames=keys)
    	dict_writer.writeheader()
    	dict_writer.writerows(data)
    	logger.debug(out_file)
    with open("data_trends11.csv", "w+", encoding="utf-8", newline="") as out_file:
    	dict_writer = csv.DictWriter(out_file, fieldnames=keys)
    	dict_writer.writeheader()
    	dict_writer.writerows(data)
    	logger.debug(out_file)

@app.agent(data_topic, sink=[persist_data], concurrency=10)
@log
async def fetch_data(raw_data: TrendsData) -> None:
    async for data in raw_data:

        logger.debug("data fetched from the kafka stream")
        logger.debug(data)
        data = json.loads(data)
        yield data #.decode("utf-8")

if __name__ == "__main__":
    app.main()
