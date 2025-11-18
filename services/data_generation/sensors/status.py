from typing import List
import random
import numpy as np
from services.data_generation.config import sensor_config

def calculate_status(
    consumption: float, voltage: float, hour: int, base_profile: List = sensor_config.BASE_PROFILE
):
    base = base_profile[hour]

    rare_failure = 0 
    spike_anomaly = 0
    low_consumption = 0
    voltage_anomaly = 0 

    # Rare failure / dropout
    if random.random() < 0.0005:
        rare_failure = 1 << 0

    # Spike anomaly
    if abs(consumption - base) > 3 * 0.1 * base:
        spike_anomaly = 1 << 1

    # low consumption
    if consumption < 0.05 * base:
        low_consumption = 1 << 2

    # volatage anomaly
    if voltage < sensor_config.VOLTAGE_RANGE[0] or voltage > sensor_config.VOLTAGE_RANGE[1]:
        voltage_anomaly = 1 << 3

    status = rare_failure | spike_anomaly | low_consumption | voltage_anomaly

    return status
