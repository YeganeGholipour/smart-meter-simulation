from typing import List
import random
import numpy as np
from services.data_generation.config import sensor_config

def calculate_status(
    consumption: float, voltage: float, hour: int, base_profile: List = sensor_config.BASE_PROFILE
):
    base = base_profile[hour]
    status = 0  # default normal

    # Rare failure / dropout
    if random.random() < 0.0005:
        status = 2

    # Spike anomaly
    elif abs(consumption - base) > 3 * 0.1 * base:
        status = 1

    # low consumption
    elif consumption < 0.05 * base:
        status = 3

    # volatage anomaly
    elif voltage < sensor_config.VOLTAGE_RANGE[0] or voltage > sensor_config.VOLTAGE_RANGE[1]:
        status = 4

    return status
