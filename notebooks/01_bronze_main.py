# Databricks notebook source
import sys
import uuid

PROJECT_ROOT = "/Workspace/Users/luisferlc1515@hotmail.com/Wanderbricks-project-Databricks"
sys.path.append(PROJECT_ROOT)

from src.core.config_loader import load_config, validate_config, get_enabled_tables, validate_table_config
from src.bronze.ingestion import ingest_table

CONFIG_PATH = f"{PROJECT_ROOT}/config/bronze_table_config.json"

config = load_config(CONFIG_PATH)
validate_config(config)
validate_table_config(
    config,
    [
        "source_table",
        "target_table",
        "key_columns"
    ]
)

tables = get_enabled_tables(config)

RUN_ID = str(uuid.uuid4())

# Create catalog and schemas if not exist

target_catalog = config["target"]["catalog"]
bronze_schema = config["target"]["bronze_schema"]
audit_schema = config["target"]["audit_schema"]
quarantine_schema = config["target"]["quarantine_schema"]

# Create catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")

# Create schemas
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{bronze_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{audit_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{quarantine_schema}")
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS
    {target_catalog}.{audit_schema}.bronze_ingestion_log
    (
        table_name STRING,
        status STRING,
        row_count INT,
        load_timestamp TIMESTAMP
    )
    USING DELTA
    """
)

print("✅ Catalog and schemas and audit table created (if not existing)")

for t in tables:
    ingest_table(spark, t, config, RUN_ID)