# Sisteme Big Data pentru Machine Leaning

## Pipeline ETL Distribuit: Migrare PostgreSQL ➔ MongoDB Sharded Cluster

This project implements a End-to-End Data Pipeline focused on the migration and transformation of data from a relational (SQL) structure to a distributed NoSQL architecture.

### Data Modeling & Transformation

Unlike the relational model in PostgreSQL where data is normalized across 5 tables (users, products, carts, cart_items reviews), the MongoDB implementation focuses on data locality and read performance:

-   Collection orders: Consolidates carts, cart_items, and users. It uses Document Embedding for items (an array of product sub-documents).

-   Collection products_catalog: Consolidates products and reviews, embedding all reviews directly into the product document to eliminate the need for JOINs.

### Tech Stack

-   Storage: PostgreSQL, MongoDB

-   Ingestion: Python, psycopg2, pymongo, requests

-   Management: Mongo Express (UI for MongoDB), Docker & Docker Compose

-   Orchestration: Apache Airflow (In Development)

### System Architecture

The entire system is orchestrated via Docker and follows a robust ETL (Extract, Transform, Load) lifecycle:

```
    A[API/Scraping] --> B(PostgreSQL - Raw/Staging)
    B --> C{Apache Airflow} (In Development)
    C --> D[Data Transformation] (In Development)
    D --> E[MongoDB]
```

-   Extraction (Ingestion): Python scripts fetch raw data from external APIs or web scraping.

-   Staging (PostgreSQL): Raw data is persisted into a normalized SQL schema to ensure data integrity and auditability.

-   Transformation: Using Python (Pandas/PyMongo), data is cleaned and denormalized. We convert relational rows into rich, nested BSON documents.

-   Loading (Distributed): Processed data is loaded into MongoDB 

-   Orchestration: Apache Airflow manages the workflow, handling retries, task dependencies, and monitoring.

OBS:

-   Transformation and Orchestration stages are in development