# Airflow-ETL
ETL pipeline for Rick and Morty characters data (Airflow DAG)

# Rick and Morty ETL Pipeline
Airflow DAG for loading characters data from Rick and Morty API to PostgreSQL

## Features
- Daily automated data updates
- Pagination handling
- Bulk insert with conflict resolution
- Idempotent workflow (table creation + truncation)

## Technologies
- Apache Airflow
- PostgreSQL
- Python (requests, psycopg2)
