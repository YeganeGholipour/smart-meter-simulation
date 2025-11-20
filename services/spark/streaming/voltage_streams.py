from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col, window, avg


def meter_hourly_voltage_monitoring(df: SparkDataFrame) -> SparkDataFrame:
    hourly_house_voltage = house_agg = (
        df.withWatermark("timestamp", "2 hour")
        .groupBy("meter_id", window("timestamp", "1 hour"))
        .agg(
            avg("voltage_v").alias("avg_voltage"),
            max("voltage_v").alias("max_voltage"),
            min("voltage_v").alias("min_voltage"),
            sum("voltage_v").alias("sum_voltage"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop(col("window"))
    )

    return hourly_house_voltage
