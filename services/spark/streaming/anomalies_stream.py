from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import getbit, col, lit, window, avg, expr, stddev, when, min, max, sum

from services.spark.common.utils import status_processing


def anomaly_detection_meter(df: SparkDataFrame) -> SparkDataFrame:
    anomaly = (
        df.withWatermark("event_ts", "1 minute")
        .groupBy(window("event_ts", "30 seconds"), col("meter_id"))
        .agg(
            avg("power_kw").alias("avg_power"),
            max("power_kw").alias("max_power"),
            min("power_kw").alias("min_power"),
            expr("bit_or(status)").alias("window_status"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop(col("window"))
    )

    anomaly = status_processing(anomaly)

    return anomaly


def anomaly_prediction_features(df: SparkDataFrame) -> SparkDataFrame:
    prediction = (
        df.withWatermark("event_ts", "10 minutes")
        .groupBy(window("event_ts", "5 minutes"), col("meter_id"))
        .agg(
            avg("power_kw").alias("power_5m_avg"),
            stddev("power_kw").alias("power_5m_std"),
            avg("voltage_v").alias("voltage_5m_avg"),
            stddev("voltage_v").alias("voltage_5m_std"),
            expr("bit_or(status)").alias("window_status"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop(col("window"))
    )

    prediction = status_processing(prediction)

    return prediction


def anomaly_count_hourly_per_building(df: SparkDataFrame) -> SparkDataFrame:
    anomaly_count_building = (
        df.withWatermark("event_ts", "2 hour")
        .groupBy(window("event_ts", "1 hour"), col("building_id"))
        .agg(
            expr("bit_or(status)").alias("window_status"),
        )
    )

    anomaly_count_building = (
        anomaly_count_building.groupBy("building_id", "window")
        .agg(
            sum(when(getbit(col("window_status"), lit(0)) == 1, 1).otherwise(0)).alias(
                "rare_failure_count"
            ),
            sum(when(getbit(col("window_status"), lit(1)) == 1, 1).otherwise(0)).alias(
                "spike_anomaly_count"
            ),
            sum(when(getbit(col("window_status"), lit(2)) == 1, 1).otherwise(0)).alias(
                "low_consumption_count"
            ),
            sum(when(getbit(col("window_status"), lit(3)) == 1, 1).otherwise(0)).alias(
                "voltage_anomaly_count"
            ),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop(col("window"))
    )

    return anomaly_count_building
