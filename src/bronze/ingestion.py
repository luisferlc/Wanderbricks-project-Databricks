from src.utils.metadata import add_metadata
from src.utils.validation import split_rejects
from src.utils.audit import log_audit


def ingest_table(
    spark,
    config,
    pipeline_config,
    run_id
):

    source = (
        f"{pipeline_config['source']['catalog']}."
        f"{pipeline_config['source']['schema']}."
        f"{config['source_table']}"
    )

    target = (
        f"{pipeline_config['target']['catalog']}."
        f"{pipeline_config['target']['bronze_schema']}."
        f"{config['target_table']}"
    )

    reject = (
        f"{pipeline_config['target']['catalog']}."
        f"{pipeline_config['target']['quarantine_schema']}."
        f"{config['target_table']}_reject"
    )

    audit_table = (
        f"{pipeline_config['target']['catalog']}."
        f"{pipeline_config['target']['audit_schema']}."
        f"bronze_ingestion_log"
    )

    try:

        print(f"Processing: {source}")

        df = spark.table(source)

        df = add_metadata(
            df,
            run_id,
            pipeline_config["load"]["load_type"],
            source
        )

        valid_df, reject_df = split_rejects(
            df,
            config.get("key_columns")
        )

        valid_df.write.mode(
            "overwrite"
        ).saveAsTable(target)

        if reject_df is not None:

            reject_df.write.mode(
                "overwrite"
            ).saveAsTable(reject)

        row_count = valid_df.count()

        log_audit(
            spark=spark,
            table=target,
            status="SUCCESS",
            count=row_count,
            audit_table=audit_table
        )

        print(
            f"SUCCESS -> {target} "
            f"({row_count} rows)"
        )

    except Exception as e:

        log_audit(
            spark=spark,
            table=target,
            status="FAILED",
            count=0,
            audit_table=audit_table
        )

        print(
            f"FAILED -> {target}"
        )

        raise e