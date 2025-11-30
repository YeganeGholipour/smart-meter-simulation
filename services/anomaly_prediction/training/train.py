from services.anomaly_prediction.common.read_clickhouse import read_from_clickhouse
from services.anomaly_prediction.features.labels import build_labels
from services.anomaly_prediction.features.feature import build_features_training
from services.anomaly_prediction.training.train_data import training_data
from services.anomaly_prediction.training.train_data import train_model
from services.anomaly_prediction.training.evaluation import evaluate_model, metrics, roc


def main():
    df = read_from_clickhouse()
    labels_df, w = build_labels(df)
    print("=== LABELS ===")
    labels_df.show(20, truncate=False)

    features_df = build_features_training(labels_df)
    print("=== FEATURES ===")
    features_df.show(20, truncate=False)

    training_df = training_data(features_df)

    predictions, model = train_model(training_df)
    print("=== SAMPLE PREDICTIONS ===")
    predictions.show(20, truncate=False)

    score = evaluate_model(predictions)
    print("=== MODEL SCORE ===")
    print("AUPR:", score)

    metric = metrics(predictions)
    print("=== CONFUSION MATRIX ===")
    print(metric.confusionMatrix().toArray())

    print("=== PRECISION ===")
    print("Precision:", metrics.precision(1.0))
    print("Recall:", metrics.recall(1.0))
    print("F1:", metrics.fMeasure(1.0))

    print("=== ROC CURVE ===")
    roc(predictions)


    predictions.write.parquet("services/anomaly_prediction/predictions/spike_predictions.parquet", mode="overwrite")

    model.save("services/anomaly_prediction/models/spike_rf_model")
    return score


if __name__ == "__main__":
    main()