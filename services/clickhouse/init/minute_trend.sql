CREATE TABLE IF NOT EXISTS meter_minute_trend
(
    window_start DateTime,
    window_end   DateTime,
    meter_id     UInt32,

    avg_power_min   Float32,
    avg_voltage_min Float32,
    event_count     UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (meter_id, window_start);