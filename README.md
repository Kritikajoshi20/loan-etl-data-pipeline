**Loan ETL Data Pipeline**

This project implements an automated ETL (Extract, Transform, Load) data pipeline for processing loan datasets using modern data engineering tools. The pipeline is designed to be scalable, modular, and production-oriented, suitable for real-world analytics use cases.

**🛠️ Tech Stack**

1. **Apache Airflow** – Workflow orchestration & scheduling
2. **Apache Spark (PySpark)** – Distributed data processing & transformations
3. **Docker & Docker Compose** – Containerized deployment
4. **MinIO (S3-compatible storage)** – Raw & processed data storage
5. **PostgreSQL** – Metadata database & analytics tables

**Pipeline Workflow**

1. Ingest raw loan data files (CSV)
2. Validate schema and perform data quality checks
3. Clean and transform data using PySpark
4. Store curated data in MinIO (S3)
5. Load final datasets into PostgreSQL
6. Monitor and track execution via Airflow

**🔐 Key Features**

1. Fully containerized environment
2. Scalable Spark-based transformations
3. Secure handling of secrets via environment variables
4. Modular DAG design for easy extension
