from confluent_kafka import Consumer, KafkaException
from shared_lib.models.schema import MeterEvent
from config import consumer_conf, config
import json

consumer = Consumer(consumer_conf)
consumer.subscribe([config.topic])


def collect_data():
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                print("Consumer error:", msg.error())
                continue

            try:
                raw_data = json.loads(msg.value().decode("utf-8"))
                event = MeterEvent(**raw_data)

                print(f"Consumed valid event: {event}")

                consumer.commit(msg)
            except Exception as e:
                print(f"Invalid message: {e}")
                print(f"Raw data: {msg.value()}")

    except KeyboardInterrupt:
        print("Stopping consumer...")

    finally:
        consumer.close()
        print("Consumer closed.")
