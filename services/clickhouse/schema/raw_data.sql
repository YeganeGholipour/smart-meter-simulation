CREATE TABLE raw_data (
    meter_id UInt32,
    building_id UInt32,
    event_time DateTime,

    power_kw Float32,
    voltage_v Float32,
    event_status UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (meter_id, event_time);

