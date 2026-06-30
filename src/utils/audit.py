def log_audit(spark, table, status, count):
    spark.sql(f"""
        INSERT INTO medallion_demo.audit.bronze_ingestion_log
        VALUES ('{table}', '{status}', {count}, current_timestamp())
    """)