from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import window, col, max, min, avg, sum


def meter_hourly_power_consumption(df: SparkDataFrame) -> SparkDataFrame:
    hourly_house_power_consumption = (
        df.withWatermark("event_ts","15 minutes")
        .groupBy("meter_id", window("event_ts", "1 hour"))
        .agg(
            avg("power_kw").alias("avg_power"),
            max("power_kw").alias("max_power"),
            min("power_kw").alias("min_power"),
            sum("power_kw").alias("total_power"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop(col("window"))
    )

    return hourly_house_power_consumption
