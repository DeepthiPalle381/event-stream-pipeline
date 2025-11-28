# Event Stream Pipeline (User Activity Logs)

This project simulates a streaming-style data pipeline for user activity logs (clickstream / e-commerce events). It processes time-stamped events in micro-batches, builds session-level and aggregated metrics, and applies data quality checks.

## Goals
- Ingest raw event data
- Build bronze, silver, and gold layers
- Compute time-based metrics (events per minute/hour)
- Build funnels (view → cart → purchase)
- Add data quality tests and orchestration

## Tech Stack
- Python (Pandas)
- SQL
- Pytest
- Airflow (optional, later)

## Data
The dataset comes from [Kaggle ...] (add exact name later).
Place the events file in `data/raw/events_raw.csv`.

