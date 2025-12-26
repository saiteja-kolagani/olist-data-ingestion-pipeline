# Reliable Daily Batch Data Ingestion Pipeline – Olist Orders

## Overview
This project implements a reliable ingestion pipeline for e-commerce order data.
It validates schema, enforces data quality, quarantines invalid records, and provides observability via structured logging.

## Dataset
- Olist Brazilian E-Commerce Dataset (Orders)
- Source: Kaggle

## Pipeline Stages
1. Ingestion Gate
   - Schema validation
   - Mandatory field validation
   - Valid / Invalid separation
   - Metrics logging

## Tech Stack
- Python
- CSV
- Logging

## Future Enhancements
- Deduplication
- Incremental processing
- AWS S3 & Glue integration
