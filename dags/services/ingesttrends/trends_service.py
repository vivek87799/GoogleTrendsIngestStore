#!/usr/bin/env python3

# Logical implementaion
# 1) create a payload
#
# 2) request data for the give keywords
#
# 3) push the fetched data into kafka topic

import json
import pandas as pd
from typing import List
from pytrends.request import TrendReq
from services.ingesttrends.config import KafkaConfig, Parameters, Fields
# importing the custom file and stream logger
from services.ingesttrends.helper_functions import log, logger
from services.ingesttrends.kafka_publisher import KafkaPublisher


class GoogleTrendsService:
    """
    A class to connect to the Google Trends API and push data to Kafka

    ...

    Attributes
    ----------
    hl : str
        host language
    tz : int
        time zone
    age : int
        age of the person

    Methods
    -------
    build_payload(hl, tz):
        Sets the search interest
    get_data():
        Gets the queried data as pandas dataframe
    """

    @log
    def __init__(self, hl: str=Parameters.LANGUAGE, tz=Parameters.TIME_ZONE) -> None:
        """
        Initializes the connection to pytrends API and Kafka broker
        """
        self.pytrends_connection = TrendReq(hl=hl, tz=tz)
        self.kafka_pub = KafkaPublisher()
    
    @log
    def build_payload(self, kw_list: List[str], timeframe: str) -> None:
        """
        Sets the search interest

        Attributes
        ----------
        """
        self.pytrends_connection.build_payload(kw_list=kw_list, timeframe=timeframe)
    
    @log
    def get_data(self) -> pd.DataFrame:
        """
        Retrives the query set using the build_payload as pandas dataframe
        """
        return self.pytrends_connection.interest_over_time()
    
    @log
    def push_data(self,  df: pd.DataFrame, topic: str = KafkaConfig.TOPIC) -> str:
        # Resetting default index of date to a column
        df = df.reset_index()
        # Replace space with _
        df.columns = df.columns.str.replace(' ', '_')
        # formating datetime to string for serialization
        df[Fields.DATE] = df[Fields.DATE].dt.strftime(Fields.DATE_FORMAT)
        df.drop(columns="isPartial", inplace=True)
        # converting pandas dataframe to a dict of dict
        message_list = df.to_dict(Fields.RECORDS)
        #  dict to strings
        message = json.dumps(message_list)
        logger.debug("check the message to be published to the kafka topic")
        logger.debug(message_list)
        self.kafka_pub.publish_message(topic, message)
        return message
        
    
    @log
    def run_manager(self) -> None:
        """
        Runs the service to retirive the data for the requested period and push the result to the kafka topic
        """
        self.pytrends_connection.build_payload(kw_list=Parameters.KW_LIST, timeframe=Parameters.TIMEFRAME)
        df = self.get_data()
        message = self.push_data(df)
        return message


if __name__ == "__main__":
    trendsAPI = GoogleTrendsService()
    trendsAPI.run_manager()
