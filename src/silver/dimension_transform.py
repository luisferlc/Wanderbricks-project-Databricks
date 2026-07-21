from src.silver.common import (
    standardize_column_names,
    trim_string_columns,
    lowercase_email_columns,
    round_float_columns
)

from src.utils.validation import (
    validate_required_columns,
    remove_duplicates,
    split_rejects
)


def transform_dimension(
    df,
    key_columns
):
    """
    Generic Silver dimension transformation.

    Returns:
        valid_df,
        reject_df
    """

    # -------------------------
    # Standardize columns names
    # -------------------------

    df = standardize_column_names(df)

    # -------------------------
    # Validate required columns
    # -------------------------

    validate_required_columns(
        df,
        key_columns
    )

    # -------------------------
    # Remove spaces
    # -------------------------

    df = trim_string_columns(df)

    # -------------------------
    # Normalize emails
    # -------------------------

    df = lowercase_email_columns(df)

    # -------------------------
    # Round float columns
    # -------------------------
    
    df = round_float_columns(df)

    # -------------------------
    # Rejects
    # -------------------------

    valid_df, reject_df = split_rejects(
        df,
        key_columns
    )

    # -------------------------
    # Remove duplicates
    # -------------------------

    valid_df = remove_duplicates(
        valid_df,
        key_columns
    )

    return (
        valid_df,
        reject_df
    )