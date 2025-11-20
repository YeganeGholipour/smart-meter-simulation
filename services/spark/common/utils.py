from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import getbit, col, lit


def status_processing(df: SparkDataFrame) -> SparkDataFrame:
    processed_df = (
        df.withColumn("rare_failure", getbit(col("window_status"), lit(0)) != 0)
        .withColumn("spike_anomaly", getbit(col("window_status"), lit(1)) != 0)
        .withColumn("low_consumption", getbit(col("window_status"), lit(2)) != 0)
        .withColumn("voltage_anomaly", getbit(col("window_status"), lit(3)) != 0)
    )

    return processed_df
