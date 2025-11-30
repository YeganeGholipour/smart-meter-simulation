from confluent_kafka import Producer
import time
from services.data_generation.sensors.generation import generate_meter_reading
from shared_lib.models.schema import MeterEvent
from services.data_generation.config import kafka_config

producer_conf = {"bootstrap.servers": kafka_config.brokers, "client.id": kafka_config.client_id}

producer = Producer(producer_conf)


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")


def send_event(event: MeterEvent):
    key = f"building_{event.building_id}".encode()
    value = event.model_dump_json().encode("utf-8")

    producer.produce(topic=kafka_config.topic, key=key, value=value, callback=delivery_report)
    producer.poll(0)


def produce_event():
    buildings = 10
    homes_per_building = 10

    while True:
        for building in range(1, buildings + 1):
            for home in range(1, homes_per_building + 1):
                metrics = generate_meter_reading(
                    meter_id=(building - 1) * homes_per_building + home,
                    building_id=building,
                )
                event = MeterEvent(**metrics)
                send_event(event)

        print("Sleeping for 5 seconds...")

        time.sleep(5)



if __name__ == "__main__":
    print("Running data generation ...")
    try:
        produce_event()
    except Exception as e:
        import traceback
        print("Error in data generation:", e)
        traceback.print_exc()

# TODO: 
# Use asyncio / threading
# pre-calculate the timestamp
# Add error handling
# Add logging

