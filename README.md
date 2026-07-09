# GitOps Demonstration Sandbox: Airflow 3 (GitSync) + Apache Spark + MinIO S3

This project is a **production-grade, cloud-native** demonstration stand designed to emulate an enterprise Big Data infrastructure inside an isolated Docker ecosystem.

It showcases a fully decoupled **GitOps Code Deployment pipeline** working in perfect synchronization with a robust **Data Lakehouse (Medallion Architecture)** batch-processing workflow.

## Architecture 

The platform architecture completely decouples the GitOps code deployment lifecycle from the distributed data processing pipeline.

### 1. Code Deployment 
DAGs and Spark applications are pulled automatically from GitHub into an isolated, immutable storage volume:
```text
[Local IDE] ──(git push)──> [GitHub] ──(60s poll)──> [Git-Sync Sidecar] ──(Read-Only Vol)──> [Airflow / Spark]
```

### 2. Data Processing
Data transitions incrementally through three decoupled validation layers within the local S3 lakehouse:
```text
[Crypto API] ──> [MinIO: Bronze (JSON)] ──(Spark ETL)──> [MinIO: Silver (Parquet)] ──(Spark Agg)──> [MinIO: Gold (Marts)]
```

## Project Directory Map

```
.
├── dags/
│   └── crypto_data_pipeline.py    # Master pipeline DAG utilizing Airflow 3 Asset Awareness
├── src/
│   └── spark_jobs/
│       ├── silver_transformer.py  # Spark core ETL logic (stateless data sanitization)
│       └── gold_aggregator.py     # Spark analytical logic (business data mart generation)
├── .env.example                   # Infrastructure deployment context variables template
├── docker-compose.yaml            # Master multi-container cloud-native orchestration blueprint
├── Dockerfile                     # Customized Airflow image injected with Java 17 & Spark client binaries
└── README.md                      # Platform technical documentation
```

## Infrastructure Stack

* **Orchestration:** Apache Airflow 3.2.2 — Workflow engine with isolated scheduler, webserver, triggerer, and dag-processor components.
* **Code Delivery:** Git-Sync v4.2.1 — Sidecar container ensuring runtime code immutability via secure background repository sync.
* **Compute Cluster:** Apache Spark 3.5.1 — Distributed Master/Worker cluster for offloading heavy ETL workloads from Airflow.
* **Object Storage:** MinIO Server — High-performance, local S3-compatible object storage infrastructure.
* **Metadata DB:** PostgreSQL 15 — Relational backend persistence for Airflow internal state metadata.

## Getting Started 

The entire platform is completely autonomous. To run this sandbox on any machine, ensure you have **Docker Desktop** (with Compose V2) and **Git CLI** active.

### Phase 1: Installation & Setup
1. Clone the repository:
   ```bash
   git clone https://github.com/olga-a-de/airflow-spark-s3_demo.git
   cd airflow-spark-s3_demo
   ```

2. Initialize your global runtime environment parameters:
   ```bash
   cp .env.example .env
   ```

3. Start all platform services:
   ```bash
   docker compose up -d --build
   ```

## Platform Entrypoints

Once the internal schema migrations finish executing (approx. 45 seconds), access the core monitoring dashboards through your local browser:

- **Airflow**: http://localhost:8082 
- **MinIO S3 Console**: http://localhost:9011 (minioadmin/minioadmin)
- **Spark Master Console**: http://localhost:8089

## Teardown Execution

* Standard graceful application stack shutdown:
  ```bash
  docker compose down
  ```
* Complete environment wipe:
  ```bash
  docker compose down -v
  ```
