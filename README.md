# Event Stream Pipeline (User Activity Logs)

This project simulates a streaming-style data pipeline for user activity / clickstream events.  
It builds Bronze, Silver, and Gold data layers on top of an events dataset and computes session-level and aggregated metrics.

---

## 🚀 Goals

- Ingest raw event logs and partition them into a Bronze layer
- Clean and standardize events into a Silver layer
- Build sessionized views of user behaviour
- Aggregate metrics in a Gold layer (events per minute, event type counts, funnels)
- Add data quality tests and orchestration via an Airflow DAG

---

## 🧰 Tech Stack

- **Python** (Pandas)
- **Pytest** (data quality tests)
- **SQL** (optional analytics)
- **Apache Airflow** (orchestration – DAG only)
- **Git / GitHub**

---

## 📦 Data

- Source: public events / clickstream dataset from Kaggle (e-commerce behaviour / event logs).
- Raw sample stored at: `data/raw/events_raw.csv`  
  (Full dataset is larger and not pushed to GitHub.)

Key columns used:

- `event_time` – timestamp of the event  
- `user_id` – user identifier  
- `event_type` – type of action (view, cart, purchase, etc.)  
- (plus product/category columns depending on the dataset)

---

## 🧱 Architecture

Layers:

1. **Raw**  
   - `data/raw/events_raw.csv` (sample of the full dataset)

2. **Bronze (src/ingest/ingest_events.py)**  
   - Reads raw events  
   - Parses timestamps  
   - Partitions events by date into `data/bronze/events_YYYY-MM-DD.csv`

3. **Silver (src/transform/transform_events.py)**  
   - Loads all Bronze files  
   - Standardizes `event_type` values  
   - Calculates per-user sessions (new session if gap > 30 min)  
   - Outputs:
     - `data/silver/events_silver.csv`
     - `data/silver/sessions.csv`

4. **Gold (src/transform/build_gold_tables.py)**  
   - Uses Silver events to create:
     - `data/gold/events_by_minute.csv`
     - `data/gold/events_by_type.csv`
     - `data/gold/user_funnel.csv`

5. **Orchestration (dags/event_stream_dag.py)**  
   - Airflow DAG with tasks:
     - `ingest_bronze_events`
     - `build_silver_layer`
     - `build_gold_layer`

6. **Data Quality (tests/)**  
   - Validates:
     - sessions have start/end
     - session length is non-negative
     - gold tables have valid counts and user_id column

---

## 📂 Project Structure

```text
event-stream-pipeline/
├── data/
│   ├── raw/
│   │   ├── events_raw.csv        # sampled dataset
│   │   └── events_raw_full.csv   # full dataset (ignored in git)
│   ├── bronze/                   # partitioned by date
│   ├── silver/                   # cleaned + sessions
│   └── gold/                     # aggregated metrics
├── src/
│   ├── ingest/
│   │   └── ingest_events.py
│   └── transform/
│       ├── transform_events.py
│       └── build_gold_tables.py
├── tests/
│   ├── test_silver_layer.py
│   └── test_gold_layer.py
├── dags/
│   └── event_stream_dag.py
├── requirements.txt
└── README.md

## ▶️ How to Run Locally

```bash
# 1. Clone the repository
git clone https://github.com/DeepthiPalle381/event-stream-pipeline.git
cd event-stream-pipeline

# 2. Create & activate a virtual environment (Windows)
python -m venv .venv
.venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run Bronze ingestion (raw → bronze)
python src/ingest/ingest_events.py

# 5. Run Silver transform (bronze → silver)
python src/transform/transform_events.py

# 6. Run Gold aggregation (silver → gold)
python src/transform/build_gold_tables.py

# 7. Run tests
pytest
