import random

def generate_voltage():
    voltage_v = random.gauss(230, 5)

    return round(voltage_v, 1)
