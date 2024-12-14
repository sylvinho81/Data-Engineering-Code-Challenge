from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import to_timestamp, lit
from pyspark.sql.types import StructType


def load_csv(spark: SparkSession, path: str, schema: StructType, header: bool = True, separator: str = ",") \
        -> DataFrame:
    """
    Load a CSV file into a Spark DataFrame with a specified schema.

    Args:
        spark (SparkSession): The SparkSession used to read the CSV file.
        path (str): The file path to the CSV file to be loaded.
        schema (StructType): The schema to apply to the loaded DataFrame.
        header (bool): Whether the CSV file contains a header row. Defaults to True.
        separator (str): The delimiter used to separate values in the CSV file. Defaults to a comma (`,`).

    Returns:
        DataFrame: A Spark DataFrame containing the data from the CSV file.
    """

    df = spark.read.format("csv").option("header", header).option("sep", separator).schema(schema=schema).load(path)

    return df


def _current_utc_timestamp() -> str:
    current_utc_time = datetime.utcnow().isoformat()
    return current_utc_time


def _add_dates(df: DataFrame) -> DataFrame:
    """
    Add `created_at` and `updated_at` timestamp columns to a DataFrame with the current UTC time.

    Args:
        df (DataFrame): The input DataFrame to which the date columns will be added.

    Returns:
        DataFrame: A new DataFrame with the added `created_at` and `updated_at` columns.
    """

    current_utc_time = _current_utc_timestamp()
    value_timestamp = to_timestamp(lit(current_utc_time))
    return df.withColumns({
        "created_at": value_timestamp,
        "updated_at": value_timestamp
    })
