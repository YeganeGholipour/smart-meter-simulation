#!/bin/bash
set -e

DB_NAME="stream_meter_db"

clickhouse-client --query "CREATE DATABASE IF NOT EXISTS $DB_NAME"

for sql_file in /docker-entrypoint-initdb.d/*.sql; do
    echo "Running $sql_file"
    clickhouse-client --database "$DB_NAME" --multiquery < "$sql_file"
done