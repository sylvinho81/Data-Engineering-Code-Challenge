from pyspark.sql import DataFrame
from pyspark.sql.functions import to_date, col, coalesce


def transform_dates(df: DataFrame, date_column: str) -> DataFrame:
    """
    Transforms a date column in a PySpark DataFrame to the yyyy-MM-dd format.

    Parameters:
    df (pyspark.sql.DataFrame): Input DataFrame
    date_column (str): Name of the column containing dates to be transformed

    Returns:
    pyspark.sql.DataFrame: DataFrame with the transformed date column
    """
    formats = [
        "yyyy-MM-dd",
        "yyyy/MM/dd",
        "MM/dd/yyyy",
        "dd-MM-yyyy",
        "MMMM dd, yyyy"
    ]

    df_transformed = df.withColumn(
        date_column,
        coalesce(*[to_date(col(date_column), fmt) for fmt in formats])
    )

    return df_transformed
