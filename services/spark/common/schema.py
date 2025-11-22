from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    FloatType,
    TimestampType,
)
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import from_json, col, date_trunc


schema = StructType(
    [
        StructField("meter_id", IntegerType(), nullable=False),
        StructField("building_id", IntegerType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("power_kw", FloatType(), nullable=False),
        StructField("voltage_v", FloatType(), nullable=False),
        StructField("status", IntegerType(), nullable=False),
    ]
)


def get_schema():
    return schema


def parse_raw_df(df: SparkDataFrame) -> SparkDataFrame:
    schema = get_schema()

    parsed = (
        df.selectExpr(
            "CAST(value AS STRING) AS value",
            "CAST(key AS STRING) AS key",
            "topic",
            "partition",
            "offset",
            "timestamp",
        )
        .select(from_json(col("value"), schema).alias("data"))
        .select(
            col("data.meter_id"),
            col("data.building_id"),
            col("data.timestamp").alias("event_ts"),  
            col("data.power_kw"),
            col("data.voltage_v"),
            col("data.status"),
        )
        .filter((col("status") >= 0) & (col("status") <= 4))
    )

    parsed = parsed.withColumn("event_ts", date_trunc("second", col("event_ts")))

    return parsed

