def write_dimension(
    df,
    target_table
):
    """
    Writes a Silver dimension table.

    Parameters
    ----------
    df : DataFrame
        Silver dataframe.

    target_table : str
        Fully qualified target table.
        Example:
            medallion_demo.silver.users
    """

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(target_table)
    )