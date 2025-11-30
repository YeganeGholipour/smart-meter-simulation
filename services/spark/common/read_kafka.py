from pyspark.sql import SparkSession
from services.spark.config import kafka_config, spark_config


def create_session(app_name: str, master: str):
    spark = SparkSession.builder.appName(app_name).master(master).getOrCreate()

    return spark


def read_from_kafka(
    broker: str = kafka_config.brokers, topic: str = kafka_config.topic
) -> None:
    spark = create_session(spark_config.app_name, spark_config.master)
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    return df
