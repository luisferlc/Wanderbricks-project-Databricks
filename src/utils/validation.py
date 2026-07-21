from pyspark.sql import functions as F


def split_rejects(df, keys):
    """
    Separates valid and rejected records based on null key columns.
    """

    if not keys:
        return df, None

    existing_keys = [
        k for k in keys
        if k in df.columns
    ]

    if not existing_keys:
        return df, None

    condition = None

    for k in existing_keys:

        current_condition = F.col(k).isNull()

        condition = (
            current_condition
            if condition is None
            else condition | current_condition
        )

    reject_df = df.filter(condition)

    valid_df = df.filter(~condition)

    return valid_df, reject_df


def validate_required_columns(
    df,
    required_columns
):
    """
    Ensures required columns exist.
    """

    missing_columns = [
        c
        for c in required_columns
        if c not in df.columns
    ]

    if missing_columns:

        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )


def remove_duplicates(
    df,
    key_columns
):
    """
    Removes duplicates using configured key columns.
    """

    if not key_columns:
        return df

    existing_keys = [
        c
        for c in key_columns
        if c in df.columns
    ]

    if not existing_keys:
        return df

    return df.dropDuplicates(existing_keys)