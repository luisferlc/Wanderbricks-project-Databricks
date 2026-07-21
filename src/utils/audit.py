def log_audit(
    spark,
    table,
    status,
    count,
    audit_table
):
    """
    Generic audit logger.

    Parameters
    ----------
    spark : SparkSession
    table : str
    status : str
    count : int
    audit_table : str
        Example:
        medallion_demo.audit.bronze_ingestion_log
        medallion_demo.audit.silver_ingestion_log
    """

    spark.sql(
        f"""
        INSERT INTO {audit_table}
        VALUES (
            '{table}',
            '{status}',
            {count},
            current_timestamp()
        )
        """
    )