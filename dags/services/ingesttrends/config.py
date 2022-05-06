class Fields:
    RECORDS = "records"
    INDEX = "index"
    DATE = "date"
    DATE_FORMAT = "%Y-/%m-/%d"
class Parameters:
    LANGUAGE = "en-US"
    TIME_ZONE = 360

    KW_LIST = ["Artificial Intelligence", "machine learning", "Neural Networks", "Deep Learning", "BigQuery"]
    TIMEFRAME = "today 12-m"

    LOG_FILE_PATH = "logs"
    LOG_FILE_NAME = LOG_FILE_PATH+"/"+"trends_api_service.log"
    LOG_FILE_MODE = "w"

class KafkaConfig:

    KAFKA_BOOTSTRAP_SERVERS = ["broker-1:9092"]# ["broker-1:9092","broker-2:9092","broker-3:9092"]
    KAFKA_BOOTSTRAP_PORT = 9092
    KAKFA_VERSION = (2,7,0)
    TIMEOUT = 60
    TOPIC = "rawdata"