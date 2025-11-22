CREATE TABLE IF NOT EXISTS raw_data
(
    meter_id    UInt32,
    building_id UInt32,
    event_ts  DateTime,

    power_kw    Float32,
    voltage_v   Float32,
    status UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (meter_id, event_ts);
