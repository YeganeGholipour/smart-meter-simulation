# Smart Meter Simulation: Real-time Streaming & Anomaly Prediction Pipeline

> This is an end-to-end smart meter simulation + streaming ETL + ClickHouse OLAP + Grafana dashboards + ML anomaly prediction


### Technologies Used

- Apache Kafka
- Apache Spark: Spark streaming + SparkML
- ClickHouse
- Grafana

---
## Table of contents

1. Project Summary
2. Architecture & Design 
3. Repo layout
4. Data model & Schema
5. Data Generation
6. Streaming & aggregation
7. ClicHouse
8. Grafana
9. Anomaly Prediction
10. Development
11. Limitations, Known Issues, and possible Improvements

---
## 1. Project summary

I built a full streaming data engineering pipeline that simulates IoT smart meters, ingests them into Kafka, processes them with Spark Structured Streaming, stores aggregated analytics in ClickHouse, visualizes with Grafana, and trains a RandomForest to predict / flag anomalies.

I mainly built this project because I wanted to understand main tools that are used in production for data engineering projects:

- Demonstrates _end-to-end_ applied data engineering: streaming ingestion, real-time aggregation, OLAP storage, dashboards, and ML.
- Shows competence with the production tools commonly required for modern data engineering / smart-city projects.

---
## 2. Architecture & design

```
[Data Generator (Python)]  --> Kafka topic (smart-meter-data) --> [Spark Structured Streaming]
                                              |
                                              v
                                 ClickHouse (aggregated tables)
                                              |
                                              v
                                          Grafana

```

For Anomaly prediction:

```
ClickHouse (raw_data) --> Anomaly prediction service (Spark / training) --> ClickHouse (anomaly_prediction_results)

```


##### Design Choices

- **Kafka for ingestion:** real producers/consumers model; ordering guaranteed per keyed partition.
    
- **Spark Structured Streaming for ETL & windows:** great to do expensive aggregations with low-latency 
    
- **ClickHouse for analytics storage:** columnar, fast OLAP excellent for time-series dashboards.
    
- **Grafana for visualization:** Easy connection to Cookhouse for visualizing different metrics 

- **Pydantic for schema validation on producer side:** ensures clean JSON events (simple contract without Avro complexity).
    
- **Bitwise status codes:** provides compact multi-label anomaly flags per event (rare failure, spike, low consumption, voltage anomaly). 

---
## 3. Repo Layout 

Root contains `docker-compose.yaml`, `README.md` and directories for each component.

These are the important directories, containing services, and shared libraries, as well as Jupyter Notebooks for testing:

```bash
services/
  anomaly_prediction/    # ML service: feature extraction, training, prediction
  data_generation/       # producer service: simulated smart meters -> Kafka
  kafka_worker/          # (tools/scripts for Kafka - optional)
  spark/                 # spark consumer, streaming logic, write to ClickHouse
  clickhouse/init/       # SQL table creation scripts
notebooks/               # analysis & model notebooks
config/                  # airflow.cfg (if used)
spark-kafka-maven/       # jars / helper if needed
shared_lib/              # pydantic models & shared schema

```

---
## 4. Data model & Schema

**MeterEvent**: used by producer (data-generation), enforced via Pydantic

```python
class MeterEvent(BaseModel):
    meter_id: int
    building_id: int
    timestamp: datetime
    power_kw: float
    voltage_v: float
    status: conint(ge=0, le=15)
```

**Status Bits:***
- bit 0 (1 << 0) = `rare_failure` (dropout / device failure)
- bit 1 (1 << 1) = `spike_anomaly` (sudden consumption spike)
- bit 2 (1 << 2) = `low_consumption` (very low consumption relative to base)
- bit 3 (1 << 3) = `voltage_anomaly` (voltage outside [210, 250] V)

`status` is then combined with bitwise OR:

```python
status = rare_failure | spike_anomaly | low_consumption | voltage_anomaly
```

---
## 5. Data generation

- Each simulated meter follows a _base daily profile_ (`SENSOR_BASE_PROFILE`) with hour of day effects.
    
- Add Gaussian noise (10% std) to make patterns non-deterministic.

- Insert _rare_ events (failure 0.0005 per reading) and occasional spikes (when the reading deviates by > 3*std).

- Each meter pushes one JSON event every ~5 seconds.

```python
SENSOR_BASE_PROFILE = [0.3,0.3,0.3,0.3,0.4,0.7,0.8,0.6,0.5,0.5,0.5,0.5,
                       0.6,0.6,0.7,0.8,1.0,1.2,1.1,0.9,0.7,0.5,0.4,0.3]
SENSOR_ANOMALY_PROBABILITIES = {"failure": 0.0005, "spike_multiplier": 3}
SENSOR_VOLTAGE_RANGE = [210, 250]
```

##### Producer

- `services/data_generation/producer.py` — builds `MeterEvent`, validates via Pydantic, sends to Kafka topic `smart-meter-data` with key `building_{building_id}` (ordering per building).

- Configurable env: `KAFKA_BROKERS`, `KAFKA_TOPIC`, `SLEEP_SECONDS`, `NUM_BUILDINGS`, `METERS_PER_BUILDING`.

---
## 6. Spark streaming & aggregations

- Sparks reads from Kafka Topic `smart-meter-data`
- Parses JSON ans casts types (`spark/common/schema.py`)
- **`aggregate_all(df)`** (entry point) computes the following DataFrames (keys map to ClickHouse table names):

	- `raw_data` — optional write of raw events (if desired)
	
	- `meter_hourly_power_consumption` — avg/min/max/total for meter per hour
	
	- `building_hourly_power_consumption` — same at building level
	
	- `meter_hourly_voltage_monitoring` — voltage stats per hour
	
	- `meter_minute_trend` — 1-minute rolling aggregates (avg power/voltage, event count)
	
	- `anomaly_detection_meter` — 30-second window anomaly outputs (avg/max/min + boolean flags)
	
	- `anomaly_prediction_features` — 5-minute features (avg/std/voltage) for ML
	
	- `anomaly_count_hourly_per_building` — counts per anomaly type per building per hour

---
## 7. ClickHouse

- I created a small set of practical tables. You will find SQL DDL in `services/clickhouse/init/*.sql` (these were used in the Docker init script).

- SQL files are present in `services/clickhouse/init/`. They are executed by the ClickHouse container at initialization in `docker-compose.yaml`.

---
## 8. Grafana

###### Install plugin

```
GF_INSTALL_PLUGINS: grafana-clickhouse-datasource
```

###### Some of the panels:

- Meter Minute-level Power Trend
- Meter Hourly Power Consumption
- Anomaly Detection Per Meter
- Anomalies Count Per Building Per Hour
- Voltage Trend Per Meter
- Voltage Monitoring Per Meter Per Hour

![[Screenshot from 2025-11-25 11-44-21.png]]


###### What I did:

- Variables for `building_id` and `meter_id` so I can quickly switch context.

---
## 9. Anomaly prediction

**Goal:** predict spike anomalies (or flag likely anomalous events) using extracted features.

- Spark reads `raw_data` or `meter_features_5min` from ClickHouse.

- `anomaly_prediction/features.py` constructs features (lags, rolling means, normalized values).

- Train with `RandomForestClassifier` (see `services/anomaly_prediction/training/train.py`).

- Save model to `services/anomaly_prediction/models/spike_rf_model`.

- Prediction job `services/anomaly_prediction/prediction/predict.py` loads the model, transforms incoming features, writes predictions to `anomaly_prediction_results` table in ClickHouse.


> Sample tests/results are in `notebooks/sparkML.ipynb`.

---
## 10. Development

#### Requirements

- Docker & Docker Compose

- Python 3.10 (for local dev tools; services have images)

#### Quick start

1. Clone repo:
```bash
git clone <repo> && cd smart-meter-simulation
```

2. Build images and start services:
```bash
docker compose up --build
```

This will start service:
- Kafka
- ClickHouse
- Grafana
- `data-generation`
- `spark-streaming`
- `anomaly-prediction`

You can see dashboards at: `http://localhost:3000` (admin/admin)

---
## Limitations & future work

- **This project is NOT production-ready**: simulated meters follow simplified behavior; real customers vary more.
- **Single-node Spark for demo**: In real projects, more nodes are added for faster computation, but in my project I only used one node
- **The design is not perfect**: In my future work, I aim to enhance the design and flow, and use Airflow
- **Anomaly prediction is very limited**: I need to add more features to enhance the model

---
