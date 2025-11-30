from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
from sklearn.metrics import roc_curve
import matplotlib.pyplot as plt


def evaluate_model(predictions):
    evaluator = BinaryClassificationEvaluator(
        labelCol="label_spike", rawPredictionCol="prediction", metricName="areaUnderPR"
    )

    score = evaluator.evaluate(predictions)
    return score


def metrics(predictions):
    predictionAndLabels = predictions.select("prediction", "label_spike").rdd.map(
        lambda row: (float(row.prediction), float(row.label_spike))
    )

    metrics = MulticlassMetrics(predictionAndLabels)

    return metrics


def roc(predictions):
    pdf = predictions.select("probability", "label_spike").toPandas()
    probs = pdf["probability"].apply(lambda v: float(v[1]))
    labels = pdf["label_spike"].astype(int)

    fpr, tpr, _ = roc_curve(labels, probs)

    plt.plot(fpr, tpr)
    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.title("ROC Curve")
    plt.show()
