-- Every 30s
CREATE TABLE IF NOT EXISTS anomaly_detection_meter
(
    window_start DateTime,
    window_end   DateTime,
    meter_id     UInt32,

    avg_power    Float32,
    max_power    Float32,
    min_power    Float32,
    window_status UInt8,

    rare_failure    Boolean,
    spike_anomaly   Boolean,
    low_consumption Boolean,
    voltage_anomaly Boolean
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (meter_id, window_start);


-- Every 5m
CREATE TABLE IF NOT EXISTS anomaly_prediction_features
(
    window_start DateTime,
    window_end   DateTime,
    meter_id     UInt32,

    power_5m_avg   Float32,
    power_5m_std   Float32,
    voltage_5m_avg Float32,
    voltage_5m_std Float32,

    window_status UInt8,

    rare_failure    Boolean,
    spike_anomaly   Boolean,
    low_consumption Boolean,
    voltage_anomaly Boolean
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (meter_id, window_start);


CREATE TABLE IF NOT EXISTS anomaly_count_hourly_per_building
(
    window_start DateTime,
    window_end   DateTime,
    building_id  UInt32,

    rare_failure_count     UInt32,
    spike_anomaly_count    UInt32,
    low_consumption_count  UInt32,
    voltage_anomaly_count  UInt32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (building_id, window_start);
