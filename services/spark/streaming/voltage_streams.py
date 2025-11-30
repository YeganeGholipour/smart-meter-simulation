from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col, window, avg, max, min, sum


def meter_hourly_voltage_monitoring(df: SparkDataFrame) -> SparkDataFrame:
    hourly_house_voltage = house_agg = (
        df.withWatermark("event_ts", "15 minutes")
        .groupBy("meter_id", window("event_ts", "1 hour"))
        .agg(
            avg("voltage_v").alias("avg_voltage"),
            max("voltage_v").alias("max_voltage"),
            min("voltage_v").alias("min_voltage"),
            sum("voltage_v").alias("total_voltage"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop(col("window"))
    )

    return hourly_house_voltage
