# Wanderbricks Medallion Architecture – Databricks Project

## Overview

This project implements a **Medallion Architecture (Bronze-Silver-Gold)** using Databricks and PySpark.

Current scope:
- Bronze layer ingestion (full load)
- Config-driven pipelines
- Modular architecture (reusable + scalable)

Dataset:
- Databricks sample dataset: `samples.wanderbricks`

---

## Architecture

GitHub (source of truth)
↓
Databricks Repos
↓
Main Notebook (orchestration)
↓
Ingestion Logic (modules)
↓
Utils (shared logic)
↓
Delta Tables (Bronze, Silver, Audit, Quarantine)

---

## Project Structure

```text
wanderbricks-project/
│
├── config/
│   └── bronze_table_config.json
│   └── silver_table_config.json
│
├── src/
│   ├── core/
│   │   └── config_loader.py
│   │
│   ├── utils/
│   │   ├── metadata.py
│   │   ├── validation.py
│   │   └── audit.py
│   │
│   └── bronze/
│       └── ingestion.py
│   └── silver/
│       └── common.py
│       └── dimension_ingestion.py
│       └── dimension_transform.py
│       └── dimension_writer.py
│
├── notebooks/
│   └── 01_bronze_main.py
│   └── 02_silver_dimensions_main.py
│
├── requirements.txt
└── README.md
```
---

## Key Concepts

### Bronze Layer
- Full-load ingestion
- Raw data preserved
- No transformations
- Append metadata for lineage

### Error Handling
- Reject records stored in quarantine tables
- Pipeline does NOT fail due to bad records

### Audit Logging
- Tracks ingestion runs
- Stores row counts and execution status

---

## Tables Created

### Bronze
- medallion_demo.bronze.users_raw
- medallion_demo.bronze.hosts_raw
- medallion_demo.bronze.properties_raw
- medallion_demo.bronze.destinations_raw
- medallion_demo.bronze.bookings_raw
- medallion_demo.bronze.reviews_raw

### Silver
- medallion_demo.silver.users
- medallion_demo.silver.hosts
- medallion_demo.silver.properties
- medallion_demo.silver.destinations
- medallion_demo.silver.bookings
- medallion_demo.silver.reviews

### Quarantine
- medallion_demo.bronze.users_raw_reject
- medallion_demo.bronze.hosts_raw_reject
- medallion_demo.bronze.properties_raw_reject
- medallion_demo.bronze.destinations_raw_reject
- medallion_demo.bronze.bookings_raw_reject
- medallion_demo.bronze.reviews_raw_reject

### Audit
- medallion_demo.audit.bronze_ingestion_log
- medallion_demo.audit.silver_ingestion_log

---

## How to Run

1. Clone repo in Databricks (Repos)
2. Open: notebooks/01_bronze_main.py
3. Run notebook
4. Open: notebooks/02_silver_dimensions_main.py
5.  Run notebook

---

## Future Enhancements

- Gold layer (analytics and KPIs)
