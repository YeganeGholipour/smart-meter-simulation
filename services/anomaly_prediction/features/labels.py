from pyspark.sql import functions as F, Window


def build_labels(df):
    w = Window.partitionBy("meter_id").orderBy("window_start")

    df = df.withColumn("label_spike", F.lead("spike_anomaly", 1).over(w))
    df = df.withColumn("label_low", F.lead("low_consumption", 1).over(w))
    df = df.withColumn("label_voltage", F.lead("voltage_anomaly", 1).over(w))
    df = df.withColumn("label_rare", F.lead("rare_failure", 1).over(w))

    df = df.fillna(
        0, subset=["label_spike", "label_low", "label_voltage", "label_rare"]
    )

    return df, w
