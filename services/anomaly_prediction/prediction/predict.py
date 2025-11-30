import time
from services.anomaly_prediction.common.read_clickhouse import (
    create_session,
    load_new_data,
)
from services.anomaly_prediction.common.write_clickhouse import write_to_clickhouse
from services.anomaly_prediction.features.feature import build_features_prediction
from pyspark.ml import PipelineModel
from services.anomaly_prediction.config import (
    model_config,
    clickhouse_config,
    spark_config,
)
from pyspark.sql import functions as F 

# def main():
#     df = read_from_kafka(kafka_config.brokers, kafka_config.topic)
#     parsed_df = parse_raw_df(df)
#     model = PipelineModel.load(model_config.path)

#     features_df = build_features_prediction(parsed_df)
#     predictions = model.transform(features_df)
#     result = predictions.select("meter_id", "window_start", "prediction")
#     write_to_clickhouse(result, clickhouse_config.table)


def main():
    spark = create_session(spark_config.app_name, spark_config.master)
    model = PipelineModel.load(model_config.path)

    last_ts = None

    while True:
        try:
            # ----------------------
            # Load new raw data
            # ----------------------
            raw_df = load_new_data(spark, last_ts)

            if raw_df.count() == 0:
                time.sleep(5)
                continue

            # Save max timestamp BEFORE transforming
            max_ts = raw_df.agg(F.max("event_ts")).collect()[0][0]

            # ----------------------
            # Build features
            # ----------------------
            features_df = build_features_prediction(raw_df)

            if features_df.count() == 0:
                last_ts = max_ts
                time.sleep(5)
                continue

            # ----------------------
            # Model prediction
            # ----------------------
            predictions = model.transform(features_df)

            result = predictions.select("meter_id", "window_start", "prediction")

            # ----------------------
            # Write results to ClickHouse
            # ----------------------
            write_to_clickhouse(result, clickhouse_config.table)

            # Move forward
            last_ts = max_ts

            # Wait for next cycle (30 seconds)
            time.sleep(30)

        except Exception as e:
            print("Error in loop:", e)
            time.sleep(5)
            continue


if __name__ == "__main__":
    main()
