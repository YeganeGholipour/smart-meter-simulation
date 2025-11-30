from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline


# For Spike Anomaly

def training_data(df):
    training_df = df.select(
        "power_5m_avg",
        "power_5m_std",
        "voltage_5m_avg",
        "voltage_5m_std",
        "hour",
        "day",
        "month",
        "year",
        "lag1_power_avg",
        "lag2_power_avg",
        "lag3_power_avg",
        "lag1_voltage_avg",
        "lag2_voltage_avg",
        "lag3_voltage_avg",
        "delta_power",
        "delta_voltage",
        "label_spike",
    )

    return training_df


def build_feature_vector(training_df):
    feature_cols = [c for c in training_df.columns if c != "label_spike"]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    return assembler


def build_model():
    rf = RandomForestClassifier(
        labelCol="label_spike", featuresCol="features", numTrees=200, maxDepth=10
    )

    return rf


def build_pipeline(rf, assembler):
    pipeline = Pipeline(stages=[assembler, rf])
    return pipeline


def train_model(training_df):
    training_df = training_df.withColumn("label_spike", F.col("label_spike").cast("int"))

    train, test = training_df.randomSplit([0.8, 0.2], seed=42)

    assembler = build_feature_vector(train)
    rf = build_model()
    pipeline = build_pipeline(rf, assembler)

    model = pipeline.fit(train)
    predictions = model.transform(test)

    return predictions, model


def evaluate_model(predictions):
    evaluator = BinaryClassificationEvaluator(
        labelCol="label_spike", rawPredictionCol="prediction", metricName="areaUnderPR"
    )

    score = evaluator.evaluate(predictions)
    return score
