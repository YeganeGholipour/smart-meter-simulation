from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType
from pyspark.sql import DataFrame as SparkDataFrame


schema = StructType([
    StructField("meter_id", IntegerType(), nullable=False),
    StructField("building_id", IntegerType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("power_kw", FloatType(), nullable=False),
    StructField("voltage_v", FloatType(), nullable=False),
    StructField("status", IntegerType(), nullable=False)
])


def get_schema():
    return schema


def parse_raw_df(df: SparkDataFrame) -> SparkDataFrame:
    return df.selectExpr("CAST(value AS STRING)", "CAST(key AS STRING)", "topic", "partition", "offset", "timestamp")

