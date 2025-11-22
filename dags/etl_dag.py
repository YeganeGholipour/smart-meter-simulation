# import json
# import pendulum

# from airflow.sdk import dag, task

# @dag(
#     schedule=None,
#     start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
#     catchup=False,
#     tags=["etl"],
# )
# def etl_dag():

#     from services.spark.common.read_kafka import read_kafka
#     from services.spark.common.write_clickhouse import write_clickhouse
    
#     @task
#     def extract():
#         df = read_kafka()
#         return df
    
#     @task
#     def transform(df):
#         return result

#     @task
#     def load(data: dict):
#         for table_name, df in data.items():
#             write_clickhouse(df, table_name)

#     df = extract()
#     transformed_data = transform(df)
#     load(transformed_data)