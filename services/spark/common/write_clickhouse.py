from pyspark.sql import DataFrame as SparkDataFrame

from services.spark.streaming.anomalies_stream import (
    anomaly_detection_meter,
    anomaly_prediction_features,
    anomaly_count_hourly_per_building,
)
from services.spark.streaming.buildings_stream import (
    building_hourly_power_consumption,
)
from services.spark.streaming.meters_stream import meter_hourly_power_consumption
from services.spark.streaming.trends_stream import meter_minute_trend
from services.spark.streaming.voltage_streams import meter_hourly_voltage_monitoring
from services.spark.common.schema import parse_raw_df

from concurrent.futures import ThreadPoolExecutor


def aggregate_all(df: SparkDataFrame) -> dict:
    results = {}

    df = parse_raw_df(df)

    results["raw_data"] = df
    results["meter_hourly_power_consumption"] = meter_hourly_power_consumption(df)
    results["building_hourly_power_consumption"] = building_hourly_power_consumption(df)
    results["meter_hourly_voltage_monitoring"] = meter_hourly_voltage_monitoring(df)
    results["meter_minute_trend"] = meter_minute_trend(df)
    results["anomaly_detection_meter"] = anomaly_detection_meter(df)
    results["anomaly_prediction_features"] = anomaly_prediction_features(df)
    results["anomaly_count_hourly_per_building"] = anomaly_count_hourly_per_building(df)

    return results


def write_to_clickhouse(df, table_name):
    checkpoint_path = f"/tmp/checkpoints/spark_clickhouse/{table_name}"

    query = (
        df.writeStream
        .foreachBatch(
            lambda batch_df, batch_id: batch_df.write
            .format("jdbc")
            .option("url", "jdbc:clickhouse://clickhouse-server:8123/stream_meter_db")
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
            .option("dbtable", table_name)
            .option("user", "default")
            .option("password", "")
            .mode("append")
            .save()
        )
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start()
    )

    return query



def start_all_queries(dataframes: dict):
    queries = [
        write_to_clickhouse(df, table_name) for table_name, df in dataframes.items()
    ]

    for q in queries:
        q.awaitTermination()
