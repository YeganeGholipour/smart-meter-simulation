from pyspark.sql import functions as F, Window
from pyspark.sql import DataFrame as SparkDataFrame


def build_time_features(df):
    df = df.withColumn("hour", F.hour("window_start"))
    df = df.withColumn("day", F.dayofweek("window_start"))
    df = df.withColumn("month", F.month("window_start"))
    df = df.withColumn("year", F.year("window_start"))
    return df


def build_lag_features(df):
    lags = [1, 2, 3]
    w = Window.partitionBy("meter_id").orderBy("window_start")

    for lag in lags:
        df = df.withColumn(f"lag{lag}_power_avg", F.lag("power_5m_avg", lag).over(w))
        df = df.withColumn(f"lag{lag}_power_std", F.lag("power_5m_std", lag).over(w))
        df = df.withColumn(
            f"lag{lag}_voltage_avg", F.lag("voltage_5m_avg", lag).over(w)
        )
        df = df.withColumn(
            f"lag{lag}_voltage_std", F.lag("voltage_5m_std", lag).over(w)
        )

    return df


def build_delta_features(df):
    df = df.withColumn("delta_power", F.col("power_5m_avg") - F.col("lag1_power_avg"))
    df = df.withColumn(
        "delta_voltage", F.col("voltage_5m_avg") - F.col("lag1_voltage_avg")
    )
    return df


def anomaly_prediction_features(df: SparkDataFrame) -> SparkDataFrame:
    features = (
        df.withWatermark("event_ts", "10 minutes")
        .groupBy(F.window("event_ts", "5 minutes"), F.col("meter_id"))
        .agg(
            F.avg("power_kw").alias("power_5m_avg"),
            F.stddev("power_kw").alias("power_5m_std"),
            F.avg("voltage_v").alias("voltage_5m_avg"),
            F.stddev("voltage_v").alias("voltage_5m_std"),
            F.expr("bit_or(status)").alias("window_status"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop(F.col("window"))
    )

    return features


def build_features_training(df):
    df = build_time_features(df)
    df = build_lag_features(df)
    df = build_delta_features(df)

    df = df.dropna()
    return df


def build_features_prediction(df):
    df = anomaly_prediction_features(df)
    df = build_time_features(df)
    df = build_lag_features(df)
    df = build_delta_features(df)

    df = df.dropna()
    return df
