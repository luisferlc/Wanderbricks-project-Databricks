from pyspark.sql import functions as F


def split_rejects(df, keys):
    if not keys:
        return df, None

    condition = None
    for k in keys:
        c = F.col(k).isNull()
        condition = c if condition is None else condition | c

    reject_df = df.filter(condition)
    valid_df = df.filter(~condition)

    return valid_df, reject_df