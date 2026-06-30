from pyspark.sql import functions as F


def add_metadata(df, run_id, load_type, source_table):
    return (
        df
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_load_type", F.lit(load_type))
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_source_table", F.lit(source_table))
    )