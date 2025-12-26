# Reliable Daily Batch Data Ingestion Pipeline

## Problem Statement:

Build a reliable ingestion pipeline for e-commerce order data that validates schema, enforces data quality, and separates usable records from invalid ones. 

## Data Source:

Took Olist Orders Dataset (CSV) File from Kaggle.

## Architecture:

The ingestion pipeline is designed as a daily batch data processing system that ingests raw e-commerce order data from CSV files, validates data quality, and prepares clean data for downstream processing.

Raw CSV Source
           ↓
Ingestion Gate

- File existence validation
- Schema validation
- Mandatory field validation
- Metrics & logging

    ↓

Valid Records Bucket
          ↓
Downstream Processing (future stages)

Invalid Records Bucket (Quarantine)

## Data Flow:

CSV file → Ingestion Gate → Valid / Invalid Separation → Downstream Processing

## Failure Scenarios:

Implemented the logging configurations that handle all the observations while executing the code and storing them in the logs file and logging on console/terminal.

## Reprocessing Strategy:

- Raw data is treated as **immutable**
- Pipeline execution is **idempotent**
- Failed runs can be safely re-executed without data duplication
- Invalid records are retained to allow reprocessing after fixing validation rules or upstream data issues

## Monitoring & Alerts:

### Metrics Captured

- Total records ingested
- Valid records count
- Invalid records count

### Monitoring Strategy

- Metrics are logged during every pipeline run
- Logs provide visibility into data quality trends

### Alert Conditions (Future-ready)

- Sudden drop in total records
- Spike in invalid records
- Schema validation failures

(Currently logged locally; can be extended to AWS CloudWatch)

## Trade-offs:

| Decision | Trade-off |
| --- | --- |
| CSV-based ingestion | Simple and portable, but limited scalability |
| Fail-fast schema validation | Prevents bad data, but stops pipeline execution |
| Quarantining invalid data | Improves auditability, adds storage overhead |
| Python standard library | High control and clarity, slower than Spark |