CREATE TABLE meter_hourly_voltage_monitoring (
    meter_id UInt32,
    window_start DateTime
    window_end DateTime,
    
    avg_voltage Float32,
    min_voltage Float32,
    max_voltage Float32,
    total_voltage Float32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (meter_id, window_start);