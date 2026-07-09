# Demonstration Sandbox: Airflow 3 + Apache Spark + MinIO

This project is a ready-to-use `docker-compose` sandbox demonstrating batch processing of cryptocurrency market data using the **Medallion Architecture**.

It includes an automated data ingestor, MinIO S3-compatible object storage acting as a local Data Lakehouse, the Apache Spark distributed processing engine for data transformations, and the modern Apache Airflow 3 orchestrator.

## Architecture

The data processing workflow is organized based on the tiered storage principles of a Data Lakehouse:

```text
[Public Crypto APIs] ──> [MinIO: Bronze (JSON)] ──> [Apache Spark] ──> [MinIO: Silver (Parquet)] ──> [Apache Spark] ──> [MinIO: Gold (Marts)]
```


## Project Directory Map

```
.
├── dags/
│   └── crypto_data_pipeline.py    # Master pipeline DAG defining workflow execution tracks
├── src/
│   └── spark_jobs/
│       ├── silver_transformer.py  # Spark ETL logic (schema validation & deduplication)
│       └── gold_aggregator.py     # Spark analytical logic (business marts generation)
├── .env.example                   # Baseline public infrastructure template variables
├── docker-compose.yaml            # Master multi-container orchestration blueprint
├── Dockerfile                     # Custom Airflow image bundled with Spark runtime components
└── README.md                      # Platform documentation
```

## Infrastructure Stack

* **Orchestration:** Apache Airflow 3.2.2 — Distributed workflow scheduling & state machine management.
* **Processing Engine:** Apache Spark 3.5.1 — High-performance framework for distributed data processing and batch ETL.
* **Storage Layer:** MinIO Object Store — Local enterprise-grade S3-compatible cloud storage solution.
* **Metadata Database:** PostgreSQL 15 — Relational database backend hosting internal Airflow state metadata.


## Getting Started

### Prerequisites

Ensure you have the following tools configured locally:
* **Docker Desktop** (with an active WSL2 backend engine enabled for Windows hosts)
* **Git CLI**

## Installation & Setup

1.  Clone the repository:
    ```bash
    git clone https://github.com/olga-a-de/airflow-spark-s3_demo.git
    cd airflow-spark-s3_demo
    ```

2.  Initialize local environment configurations from the template:
    ```bash
    cp .env.example .env
    ```

3.  Run all services:
    ```bash
    docker compose up -d --build
    ```

## Platform Entrypoints

Once the internal schema migrations finish executing (approx. 45 seconds), access the core monitoring dashboards through your local browser:

- **Airflow**: http://localhost:8082 
- **MinIO S3 Console**: http://localhost:9011 (minioadmin/minioadmin)
- **Spark Master Console**: http://localhost:8089


## Stopping the Project

* To stop all running platform services and remove containers:
  ```bash
  docker compose down
  ```
  
* To completely wipe the workspace (including persistent database objects, historical logs, and all data layers inside MinIO buckets):
  ```bash
  docker compose down -v
  ```
