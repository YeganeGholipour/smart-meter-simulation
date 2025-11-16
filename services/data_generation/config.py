from dotenv import load_dotenv

load_dotenv()

import json
import socket
from pydantic_settings import BaseSettings
from pydantic import field_validator
from typing import List, Tuple, Dict

class SensorConfig(BaseSettings):
    BASE_PROFILE: List[float]
    ANOMALY_PROBABILITIES: Dict[str, float]
    VOLTAGE_RANGE: Tuple[int, int]

    class Config:
        env_file = ".env"
        env_prefix = "SENSOR_"

    @field_validator("BASE_PROFILE", mode="before")
    @classmethod
    def parse_base_profile(cls, v):
        if isinstance(v, str):
            return json.loads(v)
        return v

    @field_validator("ANOMALY_PROBABILITIES", mode="before")
    @classmethod
    def parse_anomaly_probs(cls, v):
        if isinstance(v, str):
            return json.loads(v)
        return v

    @field_validator("VOLTAGE_RANGE", mode="before")
    @classmethod
    def parse_voltage_range(cls, v):
        if isinstance(v, str):
            return tuple(json.loads(v))
        return v
    

class KafkaProducerConfig(BaseSettings):
    topic: str
    brokers: str
    client_id: str = socket.gethostname()

    class Config:
        env_file = ".env"
        env_prefix = "KAFKA_"


sensor_config = SensorConfig()
kafka_config = KafkaProducerConfig()
