from data_generation.config import sensor_config
import datetime
from sensors.power import generate_power
from sensors.voltage import generate_voltage
from sensors.status import calculate_status

def generate_meter_reading(meter_id: int, building_id: int):

    time = datetime.datetime.now(datetime.timezone.utc)
    hour = time.hour

    power_kw = generate_power(hour, sensor_config.BASE_PROFILE)
    voltage_v = generate_voltage()
    status = calculate_status(power_kw, voltage_v, hour, sensor_config.BASE_PROFILE)

    return {
        "meter_id": meter_id,
        "building_id": building_id,
        "timestamp": time.isoformat(),
        "power_kw": power_kw,
        "voltage_v": round(voltage_v, 1),
        "status": status,
    }
