from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import window, col, avg, count


def meter_minute_trend(df: SparkDataFrame) -> SparkDataFrame:
    dashboard = (
        df.withWatermark("timestamp", "5 minutes")
        .groupBy(window("timestamp", "1 minute"), col("meter_id"))
        .agg(
            avg("power_kw").alias("avg_power_min"),
            avg("voltage_v").alias("avg_voltage_min"),
            count("*").alias("event_count"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop(col("window"))
    )

    return dashboard
