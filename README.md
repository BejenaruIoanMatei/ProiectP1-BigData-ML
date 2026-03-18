# Sisteme Big Data pentru Machine Leaning

## Pipeline ETL Distribuit: Migrare PostgreSQL ➔ MongoDB Sharded Cluster

This project implements a End-to-End Data Pipeline focused on the migration and transformation of data from a relational (SQL) structure to a distributed NoSQL architecture.

### System Architecture

The entire system is orchestrated via Docker and follows a robust ETL (Extract, Transform, Load) lifecycle:

```
    A[API/Scraping] --> B(PostgreSQL - Raw/Staging)
    B --> C{Apache Airflow}
    C --> D[Data Transformation]
    D --> E[MongoDB]
```

-   Extraction (Ingestion): Python scripts fetch raw data from external APIs or web scraping.

-   Staging (PostgreSQL): Raw data is persisted into a normalized SQL schema to ensure data integrity and auditability.

-   Transformation: Using Python (Pandas/PyMongo), data is cleaned and denormalized. We convert relational rows into rich, nested BSON documents.

-   Loading (Distributed): Processed data is loaded into MongoDB 

-   Orchestration: Apache Airflow manages the workflow, handling retries, task dependencies, and monitoring.

