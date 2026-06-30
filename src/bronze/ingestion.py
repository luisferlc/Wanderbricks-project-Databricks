from src.utils.metadata import add_metadata
from src.utils.validation import split_rejects


def ingest_table(spark, config, pipeline_config, run_id):

    source = f"{pipeline_config['source']['catalog']}.{pipeline_config['source']['schema']}.{config['source_table']}"
    target = f"{pipeline_config['target']['catalog']}.{pipeline_config['target']['bronze_schema']}.{config['target_table']}"
    reject = f"{pipeline_config['target']['catalog']}.{pipeline_config['target']['quarantine_schema']}.{config['target_table']}_reject"

    df = spark.table(source)

    df = add_metadata(df, run_id, "FULL", source)

    valid_df, reject_df = split_rejects(df, config.get("key_columns"))

    valid_df.write.mode("overwrite").saveAsTable(target)

    if reject_df is not None:
        reject_df.write.mode("overwrite").saveAsTable(reject)