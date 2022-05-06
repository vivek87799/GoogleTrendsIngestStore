#!/usr/bin/env python3

import sys
from pathlib import Path
import functools
import logging


from services.ingesttrends.config import Parameters

#check if the log dir exists
Path(Parameters.LOG_FILE_PATH).mkdir(parents=True, exist_ok=True)  

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler(Parameters.LOG_FILE_NAME, Parameters.LOG_FILE_MODE)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()

logger.addHandler(file_handler)
logger.addHandler(stream_handler)



def log(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"Entering function: {func.__name__}")
        args_repr = [repr(a) for a in args]
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
        signature = ", ".join(args_repr + kwargs_repr)
        logger.debug(f"function: {func.__name__} called with args {signature}")
        try:
            result = func(*args, **kwargs)
            logger.info(f"Returning from function: {func.__name__}")
            return result
        except Exception as e:
            logger.exception(f"Exception raised in {func.__name__}. exception: {str(e)}")
            # raise e
    return wrapper
