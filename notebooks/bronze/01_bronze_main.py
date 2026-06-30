# Databricks notebook source

import sys
import uuid

PROJECT_ROOT = "/Workspace/Repos/luisferlc1515@hotmail.com/wanderbricks-project"
sys.path.append(PROJECT_ROOT)

from src.core.config_loader import load_config, validate_config, get_enabled_tables
from src.bronze.ingestion import ingest_table

CONFIG_PATH = f"{PROJECT_ROOT}/config/bronze_table_config.json"

config = load_config(CONFIG_PATH)
validate_config(config)

tables = get_enabled_tables(config)

RUN_ID = str(uuid.uuid4())

for t in tables:
    ingest_table(spark, t, config, RUN_ID)