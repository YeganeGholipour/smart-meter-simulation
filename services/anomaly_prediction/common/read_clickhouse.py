from pyspark.sql import SparkSession
from services.anomaly_prediction.config import spark_config, clickhouse_config


def create_session(app_name: str, master: str):
    # jar_path = "services/anomaly_prediction/jars/clickhouse-jdbc-0.9.1-all.jar"
    spark = (
        SparkSession.builder.appName(app_name).master(master)
        # .config("spark.jars", jar_path)
        .getOrCreate()
    )
    return spark


def read_from_clickhouse():
    spark = create_session()

    df = (
        spark.read.format("jdbc")
        .option(
            "url",
            f"{clickhouse_config.server}:{clickhouse_config.port}/{clickhouse_config.database}",
        )
        .option(
            "dbtable",
            """(
                SELECT *
                FROM anomaly_prediction_features
                WHERE window_start >= now() - INTERVAL 90 DAY
                ORDER BY meter_id, window_start
            ) AS t""",
        )
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("user", clickhouse_config.user)
        .option("password", clickhouse_config.password)
        .load()
    )

    return df


def load_new_data(spark, last_ts=None):
    query = "SELECT * FROM raw_data"
    if last_ts:
        query += f" WHERE event_ts > '{last_ts}'"
    query += " ORDER BY meter_id, event_ts"

    df = (
        spark.read.format("jdbc")
        .option(
            "url",
            f"{clickhouse_config.server}:{clickhouse_config.port}/{clickhouse_config.database}",
        )
        .option("dbtable", f"({query}) AS t")
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("user", clickhouse_config.user)
        .option("password", clickhouse_config.password)
        .load()
    )
    return df



# from pyspark.sql import SparkSession
# from services.anomaly_prediction.config import kafka_config, spark_config


# def create_session(app_name: str, master: str):
#     spark = SparkSession.builder.appName(app_name).master(master).getOrCreate()

#     return spark


# def read_from_kafka(
#     broker: str = kafka_config.brokers, topic: str = kafka_config.topic
# ) -> None:
#     spark = create_session(spark_config.app_name, spark_config.master)
#     df = (
#         spark.readStream.format("kafka")
#         .option("kafka.bootstrap.servers", broker)
#         .option("subscribe", topic)
#         .option("startingOffsets", "earliest")
#         .load()
#     )

#     return df
