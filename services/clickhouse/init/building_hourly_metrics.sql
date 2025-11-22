CREATE TABLE IF NOT EXISTS building_hourly_power_consumption
(
    building_id  UInt32,
    window_start DateTime,
    window_end   DateTime,

    avg_power  Float32,
    min_power  Float32,
    max_power  Float32,
    total_power Float32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (building_id, window_start);
