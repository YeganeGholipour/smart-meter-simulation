from dotenv import load_dotenv

load_dotenv()

import socket
from pydantic_settings import BaseSettings


class KafkaWorkerConfig(BaseSettings):
    topic: str
    brokers: str
    consumer_group: str
    client_id: str = socket.gethostname()

    class Config:
        env_prefix = "KAFKA_"


config = KafkaWorkerConfig()


consumer_conf = {
    "bootstrap.servers": config.brokers,
    "group.id": config.consumer_group,
    "auto.offset.reset": "earliest",
}
