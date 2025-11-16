from typing import List
import random
import numpy as np
from services.data_generation.config import sensor_config

def generate_power(hour: int, base_profile: List = sensor_config.BASE_PROFILE):
    records = []

    base = base_profile[hour]
    noise = np.random.normal(0, 0.1)
    consumption = base + noise

    # Add evening cooking
    if 18 <= hour <= 20 and random.random() < 0.7:
        consumption += np.random.uniform(0.3, 0.7)

    consumption = max(consumption, 0.05)

    return round(consumption, 2)
