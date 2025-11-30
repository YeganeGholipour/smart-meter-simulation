from pyspark.sql import DataFrame as SparkDataFrame
from services.anomaly_prediction.config import clickhouse_config


# def write_to_clickhouse(df: SparkDataFrame, table_name: str):
#     checkpoint_path = f"/tmp/checkpoints/predictions/{table_name}"

#     def batch_writer(batch_df, batch_id):
#         (
#             batch_df.write.format("jdbc")
#             .option(
#                 "url",
#                 f"{clickhouse_config.server}:{clickhouse_config.port}/{clickhouse_config.database}",
#             )
#             .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
#             .option("user", clickhouse_config.user)
#             .option("password", clickhouse_config.password)
#             .option("dbtable", table_name)
#             .mode("append")
#             .save()
#         )

#     query = (
#         df.writeStream.foreachBatch(batch_writer)
#         .outputMode("append")
#         .option("checkpointLocation", checkpoint_path)
#         .start()
#     )

#     query.awaitTermination()


def write_to_clickhouse(df: SparkDataFrame, table_name: str):
    df.write.format("jdbc") \
        .option("url", f"{clickhouse_config.server}:{clickhouse_config.port}/{clickhouse_config.database}") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("user", clickhouse_config.user) \
        .option("password", clickhouse_config.password) \
        .option("dbtable", table_name) \
        .mode("append") \
        .save()
