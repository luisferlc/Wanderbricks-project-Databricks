from pyspark.sql import functions as F
from pyspark.sql.types import StringType, FloatType, DoubleType, DecimalType


def standardize_column_names(df):
    """
    Standardizes column names to snake_case.
    Example:

    User Name -> user_name
    PROPERTY-ID -> property_id
    """

    for old_col in df.columns:

        new_col = (
            old_col.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace(".", "_")
        )

        if old_col != new_col:
            df = df.withColumnRenamed(
                old_col,
                new_col
            )

    return df


def trim_string_columns(df):
    """
    Removes leading and trailing spaces
    from every string column.
    """

    for field in df.schema:

        if isinstance(field.dataType, StringType):

            df = df.withColumn(
                field.name,
                F.trim(F.col(field.name))
            )

    return df


def lowercase_email_columns(df):
    """
    Converts email columns to lowercase.

    Example:
    USER@MAIL.COM
    =>
    user@mail.com
    """

    email_columns = [
        c
        for c in df.columns
        if "email" in c.lower()
    ]

    for col_name in email_columns:

        df = df.withColumn(
            col_name,
            F.lower(F.col(col_name))
        )

    return df


def round_float_columns(
    df,
    decimals=2
):
    
    columns_to_transform = ["total_amount", "rating", "base_price"]
    
    for c in columns_to_transform:
        if c in df.columns:
            df = df.withColumn(
                c,
                F.round(
                    F.col(c),
                    decimals
                )
            )

    return df
