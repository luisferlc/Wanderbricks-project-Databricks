# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Tabla de Linaje de Datos — Source → Bronze → Silver
# MAGIC
# MAGIC Este notebook lee los 2 archivos JSON de configuración de la carpeta `config/`
# MAGIC (`source_to_bronze.json` y `bronze_to_silver.json`) y construye/actualiza una
# MAGIC **tabla de linaje** con una fila por cada tabla configurada, en ambas etapas
# MAGIC del pipeline medallion.
# MAGIC
# MAGIC Estructura real de cada JSON de config (confirmada):
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "source": {
# MAGIC     "catalog": "samples",
# MAGIC     "schema": "wanderbricks",
# MAGIC     "source_system": "samples.wanderbricks"
# MAGIC   },
# MAGIC   "target": {
# MAGIC     "catalog": "medallion_demo",
# MAGIC     "bronze_schema": "bronze",
# MAGIC     "audit_schema": "audit",
# MAGIC     "quarantine_schema": "quarantine"
# MAGIC   },
# MAGIC   "load": { "load_type": "FULL" },
# MAGIC   "tables": [
# MAGIC     {"source_table": "users", "target_table": "users_raw", "key_columns": ["user_id"], "enabled": true}
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Notas de diseño:
# MAGIC - El nombre del schema de destino cambia según la etapa (`bronze_schema` en
# MAGIC   source→bronze, `silver_schema` en bronze→silver) — el parseo detecta ambos.
# MAGIC - `audit_schema` / `quarantine_schema` viven dentro del bloque `target`.
# MAGIC - `load_type` es global por archivo (bloque `load`), aplica a todas las tablas
# MAGIC   de esa etapa; si en el futuro agregas `load_type` a nivel de tabla, ese valor
# MAGIC   tiene prioridad (override).
# MAGIC - El PK se lee de `key_columns` (lista) y se guarda como string separado por comas.
# MAGIC - `bronze_to_silver.json` no trae `source_system` explícito (la fuente es la
# MAGIC   propia capa bronze). Si falta, se genera un default `catalog.schema`, igual
# MAGIC   al patrón que ya usas en `source_to_bronze.json` (`"samples.wanderbricks"`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Widgets / Parámetros

# COMMAND ----------

# -------------------------------------------------------
# Project Root (igual que en 01_bronze_main y 02_silver_dimensions_main)
# -------------------------------------------------------

PROJECT_ROOT = "/Workspace/Users/luisferlc1515@hotmail.com/Wanderbricks-project-Databricks"

# -------------------------------------------------------
# Configuration Paths
# -------------------------------------------------------

config_path = f"{PROJECT_ROOT}/config"
source_to_bronze_file = "bronze_table_config.json"  # Archivo real en config/
bronze_to_silver_file = "silver_table_config.json"  # Archivo real en config/
lineage_table_name = "medallion_demo.audit.data_lineage"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Utilidades para leer y parsear los JSON de configuración

# COMMAND ----------

import json
import os
from pyspark.sql import Row, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType
)


def read_json_config(directory: str, filename: str) -> dict:
    """
    Lee un archivo JSON de configuración.
    Soporta rutas de Workspace/Repos (acceso tipo filesystem normal) y rutas de
    Volumes/DBFS (a través de dbutils.fs) como fallback.
    """
    full_path = os.path.join(directory, filename)

    # 1) Intento directo con open() -> funciona para archivos de Workspace/Repos
    try:
        with open(full_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, IsADirectoryError):
        pass

    # 2) Fallback: dbutils.fs (DBFS / Volumes, rutas tipo dbfs:/ o /Volumes/...)
    try:
        raw = dbutils.fs.head(full_path, 1024 * 1024)
        return json.loads(raw)
    except Exception as e:
        raise FileNotFoundError(
            f"No pude leer el archivo de configuración '{full_path}'. "
            f"Revisa el widget 'config_path' y el nombre del archivo. Error original: {e}"
        )


def _first_present(d: dict, keys: list, default=None):
    """Regresa el primer valor encontrado entre varias llaves alternativas posibles."""
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default


def _get_conn_info(cfg: dict, side: str) -> dict:
    """
    Extrae catalog / schema / source_system / audit_schema / quarantine_schema
    del bloque 'source' o 'target' del JSON. 'side' es 'source' o 'target'.
    El nombre del schema puede venir como 'schema', 'bronze_schema' o 'silver_schema'
    dependiendo de la etapa del pipeline.
    """
    block = cfg.get(side, {}) or {}
    catalog = _first_present(block, ["catalog", "catalog_name"], "")
    schema = _first_present(
        block, ["schema", "schema_name", "bronze_schema", "silver_schema", "gold_schema", "target_schema"], ""
    )
    source_system = _first_present(
        block, ["source_system", "system", "source_system_name", "system_name"], ""
    )
    # Default de source_system si no viene explícito (p.ej. bronze->silver): catalog.schema
    if side == "source" and not source_system and catalog and schema:
        source_system = f"{catalog}.{schema}"

    return {
        "catalog": catalog,
        "schema": schema,
        "source_system": source_system,
        "audit_schema": _first_present(block, ["audit_schema", "audit_schema_name"], ""),
        "quarantine_schema": _first_present(block, ["quarantine_schema", "quarantine_schema_name"], ""),
    }


def _get_pk(entry: dict) -> str:
    """El PK puede venir como lista o string; lo normalizamos a string separado por comas."""
    pk = _first_present(entry, ["key_columns", "pk", "primary_key", "primary_keys", "keys"], [])
    if isinstance(pk, list):
        return ",".join(str(p) for p in pk)
    return str(pk) if pk else ""


def _get_enabled(entry: dict) -> bool:
    val = _first_present(entry, ["enabled", "is_enabled", "active"], True)
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ("true", "1", "y", "yes", "si", "sí")


def _get_load_type(cfg: dict, entry: dict) -> str:
    """
    load_type puede venir a nivel de tabla (override) o a nivel de archivo,
    dentro del bloque 'load'. Si hay valor por tabla, ese gana.
    """
    table_level = _first_present(entry, ["load_type", "loadtype", "load_mode"], None)
    if table_level:
        return table_level
    load_block = cfg.get("load", {}) or {}
    return _first_present(load_block, ["load_type", "loadtype", "load_mode"], "")


def _parse_table_entry(cfg: dict, entry: dict, stage: str) -> dict:
    """Convierte una entrada de la lista 'tables' + el contexto del config en una fila de linaje."""
    src = _get_conn_info(cfg, "source")
    tgt = _get_conn_info(cfg, "target")

    return {
        "pipeline_stage": stage,  # columna extra: "source_to_bronze" | "bronze_to_silver"
        "source_catalog": src["catalog"],
        "source_schema": src["schema"],
        "source_system": src["source_system"],
        "target_catalog": tgt["catalog"],
        "target_schema": tgt["schema"],
        "audit_schema": tgt["audit_schema"],
        "quarantine_schema": tgt["quarantine_schema"],
        "load_type": _get_load_type(cfg, entry),
        "source_table": _first_present(entry, ["source_table", "source_table_name"], ""),
        "target_table": _first_present(entry, ["target_table", "target_table_name"], ""),
        "pk": _get_pk(entry),
        "enabled": _get_enabled(entry),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Lectura de los 2 JSON de configuración

# COMMAND ----------

source_to_bronze_cfg = read_json_config(config_path, source_to_bronze_file)
bronze_to_silver_cfg = read_json_config(config_path, bronze_to_silver_file)

print(f"Config Source→Bronze leída de : {os.path.join(config_path, source_to_bronze_file)}")
print(f"Config Bronze→Silver leída de : {os.path.join(config_path, bronze_to_silver_file)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Construcción del linaje (una fila por tabla, por cada etapa)

# COMMAND ----------

lineage_rows = []

for entry in source_to_bronze_cfg.get("tables", []):
    lineage_rows.append(_parse_table_entry(source_to_bronze_cfg, entry, "source_to_bronze"))

for entry in bronze_to_silver_cfg.get("tables", []):
    lineage_rows.append(_parse_table_entry(bronze_to_silver_cfg, entry, "bronze_to_silver"))

if not lineage_rows:
    raise ValueError(
        "No se encontraron entradas en la sección 'tables' de ninguno de los 2 JSON. "
        "Revisa que la llave se llame 'tables', o ajusta _parse_table_entry a tu estructura real."
    )

print(f"Total de entradas de linaje encontradas: {len(lineage_rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. DataFrame de linaje
# MAGIC Columnas: `source_catalog, source_schema, source_system, target_catalog, target_schema,
# MAGIC audit_schema, quarantine_schema, load_type, source_table, target_table, pk, enabled`
# MAGIC (+ `pipeline_stage` y `_load_timestamp` como columnas de apoyo).

# COMMAND ----------

lineage_schema = StructType([
    StructField("pipeline_stage",    StringType(),  True),
    StructField("source_catalog",    StringType(),  True),
    StructField("source_schema",     StringType(),  True),
    StructField("source_system",     StringType(),  True),
    StructField("target_catalog",    StringType(),  True),
    StructField("target_schema",     StringType(),  True),
    StructField("audit_schema",      StringType(),  True),
    StructField("quarantine_schema", StringType(),  True),
    StructField("load_type",         StringType(),  True),
    StructField("source_table",      StringType(),  True),
    StructField("target_table",      StringType(),  True),
    StructField("pk",                StringType(),  True),
    StructField("enabled",           BooleanType(), True),
])

from datetime import datetime

lineage_df = spark.createDataFrame([Row(**r) for r in lineage_rows], schema=lineage_schema)

# -------------------------------------------------------
# Obtener el timestamp más reciente de cada tabla target
# -------------------------------------------------------

print("Buscando timestamps más recientes de cada tabla...")

# Obtener timestamp actual de Python para casos fallback
current_ts = datetime.now()

timestamp_data = []
for row in lineage_rows:
    target_full_name = f"{row['target_catalog']}.{row['target_schema']}.{row['target_table']}"
    
    try:
        # Intentar obtener el MAX(_ingestion_ts) de la tabla target
        max_ts_df = spark.sql(f"""
            SELECT MAX(_ingestion_ts) as max_ingestion_ts
            FROM {target_full_name}
        """)
        
        max_ts = max_ts_df.first()["max_ingestion_ts"]
        
        if max_ts is None:
            # Si la tabla está vacía, usar NULL
            print(f"  ⚠️  {target_full_name}: tabla vacía, usando NULL")
        else:
            print(f"  ✅ {target_full_name}: {max_ts}")
            
    except Exception as e:
        # Si la tabla no existe o no tiene columna ingestion_ts, usar NULL
        print(f"  ⚠️  {target_full_name}: error ({str(e)[:50]}...), usando NULL")
        max_ts = None
    
    timestamp_data.append({
        "target_catalog": row["target_catalog"],
        "target_schema": row["target_schema"],
        "target_table": row["target_table"],
        "_load_timestamp": max_ts
    })

# Crear DataFrame con los timestamps
timestamp_df = spark.createDataFrame([Row(**t) for t in timestamp_data])

# Join con el lineage_df para agregar los timestamps
lineage_df = (
    lineage_df.join(
        timestamp_df,
        on=["target_catalog", "target_schema", "target_table"],
        how="left"
    )
)

# Mantener NULL cuando no hay timestamp disponible
# lineage_df ya tiene la columna _load_timestamp del join, con NULLs donde corresponda

print("\n✅ Timestamps actualizados con valores de ingestion_ts\n")

display(lineage_df.orderBy("pipeline_stage", "source_table"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Guardar como tabla Delta
# MAGIC Se sobreescribe completa en cada corrida, ya que es una tabla de control/linaje
# MAGIC derivada directamente de los JSON de configuración (no de datos transaccionales).

# COMMAND ----------

(
    lineage_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(lineage_table_name)
)

print(f"✅ Tabla de linaje creada/actualizada en: {lineage_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Resumen rápido

# COMMAND ----------

display(
    lineage_df.groupBy("pipeline_stage", "enabled")
    .count()
    .orderBy("pipeline_stage", "enabled")
)
