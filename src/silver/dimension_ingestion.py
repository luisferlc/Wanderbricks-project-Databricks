from src.silver.dimension_transform import transform_dimension
from src.silver.dimension_writer import write_dimension
from src.utils.audit import log_audit
from src.utils.metadata import add_metadata


def ingest_dimension(
    spark,
    table_config,
    config,
    run_id
):

    # ---------------------------------
    # Config
    # ---------------------------------

    source_catalog = config["source"]["catalog"]
    source_schema = config["source"]["schema"]

    target_catalog = config["target"]["catalog"]
    target_schema = config["target"]["silver_schema"]

    audit_schema = config["target"]["audit_schema"]

    source_table_name = table_config["source_table"]
    target_table_name = table_config["target_table"]

    key_columns = table_config["key_columns"]

    source_table = (
        f"{source_catalog}."
        f"{source_schema}."
        f"{source_table_name}"
    )

    target_table = (
        f"{target_catalog}."
        f"{target_schema}."
        f"{target_table_name}"
    )

    audit_table = (
        f"{target_catalog}."
        f"{audit_schema}."
        f"silver_ingestion_log"
    )

    try:

        print(
            f"Processing: "
            f"{source_table} -> {target_table}"
        )

        # ---------------------------------
        # Read Bronze
        # ---------------------------------

        df = spark.table(source_table)

        # -------------------
        # Add Metadata
        # -------------------
        df = add_metadata(
            df,
            run_id,
            config["load"]["load_type"],
            source_table
        )

        # ---------------------------------
        # Transform
        # ---------------------------------

        valid_df, reject_df = transform_dimension(
            df=df,
            key_columns=key_columns
        )

        # ---------------------------------
        # Write Silver
        # ---------------------------------

        write_dimension(
            valid_df,
            target_table
        )

        # ---------------------------------
        # Audit Success
        # ---------------------------------

        row_count = valid_df.count()

        log_audit(
            spark=spark,
            table=target_table,
            status="SUCCESS",
            count=row_count,
            audit_table=audit_table
        )

        print(
            f"SUCCESS -> "
            f"{target_table} "
            f"({row_count} rows)"
        )

        # ---------------------------------
        # Info Rejects
        # ---------------------------------

        if reject_df is not None:

            reject_count = reject_df.count()

            print(
                f"Rejected rows: "
                f"{reject_count}"
            )

    except Exception as e:

        log_audit(
            spark=spark,
            table=target_table,
            status="FAILED",
            count=0,
            audit_table=audit_table
        )

        print(
            f"FAILED -> "
            f"{target_table}"
        )

        raise e