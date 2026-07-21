# Databricks notebook source

import sys
import uuid

# -------------------------------------------------------
# Project Root
# -------------------------------------------------------

PROJECT_ROOT = (
    "/Workspace/Users/luisferlc1515@hotmail.com/"
    "Wanderbricks-project-Databricks"
)

sys.path.append(PROJECT_ROOT)

# -------------------------------------------------------
# Imports
# -------------------------------------------------------

from src.core.config_loader import (
    load_config,
    validate_config,
    validate_table_config,
    get_enabled_tables
)

from src.silver.dimension_ingestion import (
    ingest_dimension
)

# -------------------------------------------------------
# Load Config
# -------------------------------------------------------

CONFIG_PATH = (
    f"{PROJECT_ROOT}/config/"
    f"silver_dimension_config.json"
)

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

# -------------------------------------------------------
# Run Id
# -------------------------------------------------------

RUN_ID = str(uuid.uuid4())

print(f"RUN_ID = {RUN_ID}")

# -------------------------------------------------------
# Create Schemas
# -------------------------------------------------------

target_catalog = config["target"]["catalog"]
silver_schema = config["target"]["silver_schema"]
audit_schema = config["target"]["audit_schema"]
quarantine_schema = config["target"]["quarantine_schema"]

spark.sql(
    f"CREATE CATALOG IF NOT EXISTS {target_catalog}"
)

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS
    {target_catalog}.{silver_schema}
    """
)

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS
    {target_catalog}.{audit_schema}
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS
    {target_catalog}.{audit_schema}.silver_ingestion_log
    (
        table_name STRING,
        status STRING,
        row_count BIGINT,
        load_timestamp TIMESTAMP
    )
    USING DELTA
    """
)

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS
    {target_catalog}.{quarantine_schema}
    """
)

print(
    "✅ Catalog and Silver schemas created "
    "(if not existing)"
)

# -------------------------------------------------------
# Process Dimensions
# -------------------------------------------------------

for table in tables:

    ingest_dimension(
        spark=spark,
        table_config=table,
        config=config,
        run_id=RUN_ID
    )

print(
    "\n✅ Silver Dimension Processing Finished"
)