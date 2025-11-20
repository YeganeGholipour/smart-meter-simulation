from dotenv import load_dotenv

load_dotenv()

import socket
from pydantic_settings import BaseSettings


class SparkConfig(BaseSettings):
    master: str
    app_name: str

    class Config:
        env_file = ".env"
        env_prefix = "SPARK_"

class KafkaConsumerConfig(BaseSettings):
    topic: str
    brokers: str
    client_id: str = socket.gethostname()

    class Config:
        env_file = ".env"
        env_prefix = "KAFKA_"


spark_config = SparkConfig()
kafka_config = KafkaConsumerConfig()
