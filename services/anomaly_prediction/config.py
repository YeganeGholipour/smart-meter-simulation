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


class ClickHouseConfig(BaseSettings):
    server: str
    port: int
    user: str
    password: str
    database: str
    table: str

    class Config:
        env_file = ".env"
        env_prefix = "CLICKHOUSE_"


class ModelConfig(BaseSettings):
    path: str

    class Config:
        env_file = ".env"
        env_prefix = "MODEL_"


class KafkaConsumerConfig(BaseSettings):
    topic: str
    brokers: str
    client_id: str = socket.gethostname()

    class Config:
        env_file = ".env"
        env_prefix = "KAFKA_"


kafka_config = KafkaConsumerConfig()
spark_config = SparkConfig()
clickhouse_config = ClickHouseConfig()
model_config = ModelConfig()
