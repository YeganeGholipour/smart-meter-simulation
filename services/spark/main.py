from services.spark.common.read_kafka import read_from_kafka
from services.spark.common.write_clickhouse import start_all_queries, aggregate_all


def main():
    df = read_from_kafka()
    dataframes = aggregate_all(df)
    start_all_queries(dataframes)


if __name__ == "__main__":
    main()

