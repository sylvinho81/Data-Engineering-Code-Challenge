from typing import Optional, List

from delta import *
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException


def _create_delta_lake_table(
    spark_session: SparkSession,
    delta_path: str,
    schema_table: StructType,
    partition_columns: Optional[List[str]] = None,
) -> None:
    """
    Create a Delta table at the specified path if it does not already exist, using the provided schema
    and optional partitioning.

    Args:
        spark_session (SparkSession): The SparkSession used to interact with the Delta Lake.
        delta_path (str): The path where the Delta table will be created.
        schema_table (StructType): The schema of the Delta table to be created.
        partition_columns (Optional[List[str]]): A list of column names to use for partitioning the data in the Delta
        table.
        If None, no partitioning will be applied.

    Returns:
        None: This function creates the Delta table in place.
    """

    try:
        DeltaTable.forPath(spark_session, delta_path)
    except AnalysisException:
        table_name = delta_path.split("/")[-1]
        builder = (
            DeltaTable.create(spark_session)
            .tableName(f"default.{table_name}")
            .addColumns(schema_table)
            .location(delta_path)
        )
        if partition_columns:
            builder = builder.partitionedBy(*partition_columns)

        builder.execute()


def _store_delta_table_by_append(df: DataFrame, delta_path: str, partition_columns: Optional[List[str]] = None) -> None:
    """
    Append a DataFrame to a Delta table, optionally partitioning the data.

    Args:
        df (DataFrame): The input DataFrame to be appended to the Delta table.
        delta_path (str): The path to the Delta table where the data will be appended.
        partition_columns (Optional[List[str]]): A list of column names to use for partitioning the data
            in the Delta table. If None, no partitioning will be applied.

    Returns:
        None: This function writes the DataFrame to the Delta table in append mode.
    """
    if partition_columns:
        df.write.mode("append").partitionBy(*partition_columns).format("delta").save(delta_path)
    else:
        df.write.mode("append").format("delta").save(delta_path)


def _store_delta_table_by_overwrite(df: DataFrame, delta_path: str, partition_columns: Optional[List[str]] = None) \
        -> None:
    """
    Overwrite a Delta table with the contents of a DataFrame, optionally partitioning the data.

    Args:
        df (DataFrame): The input DataFrame to be written to the Delta table.
        delta_path (str): The path to the Delta table where the data will be written.
        partition_columns (Optional[List[str]]): A list of column names to use for partitioning the data
            in the Delta table. If None, no partitioning will be applied.

    Returns:
        None: This function writes the DataFrame to the Delta table in overwrite mode.
    """

    if partition_columns:
        df.write.mode("overwrite").partitionBy(*partition_columns).format("delta").save(delta_path)
    else:
        df.write.mode("overwrite").format("delta").save(delta_path)


def _store_delta_table_by_merge(
    df: DataFrame,
    delta_path: str,
    spark_session: SparkSession,
    columns_match: List[str],
    columns_update: List[str]
) -> None:
    """
    Merge a DataFrame into a Delta table, updating existing rows or inserting new ones based on the specified conditions

    Args:
        df (DataFrame): The input DataFrame containing the updates to be applied to the Delta table.
        delta_path (str): The path to the Delta table where the merge operation will be performed.
        spark_session (SparkSession): The SparkSession used to access the Delta table.
        columns_match (List[str]): A list of column names to use for matching rows between the input DataFrame (`df`)
            and the target Delta table. The rows are matched using an AND condition on these columns.
        columns_update (List[str]): A list of column names to update in the target Delta table if the matching rows
            are found and the values differ. An additional column, `updated_at`, is included by default in
            `columns_update` to track when rows are updated.

    Returns:
        None: This function performs the merge operation on the Delta table in place.
    """

    delta_table = DeltaTable.forPath(spark_session, delta_path)
    match_condition = " AND ".join(f"dest.{column} = updates.{column}" for column in columns_match)

    # Create a condition to check if any of the update columns have changed
    update_condition = " OR ".join(f"dest.{column} != updates.{column}" for column in columns_update)
    columns_update.append("updated_at")

    delta_table.alias("dest").merge(df.alias("updates"), match_condition).whenMatchedUpdate(
        condition=update_condition, set={column: f"updates.{column}" for column in columns_update}
    ).whenNotMatchedInsertAll().execute()


def load_delta_lake_table(spark: SparkSession, delta_table_path: str) -> DataFrame:
    """
    Load a delta lake table from delta_table_path

    Parameters:
    spark (SparkSession): The Spark session.
    delta_table_path (str): Path to the delta lake table.

    Returns: The DataFrame
    """

    return spark.read.format("delta").load(delta_table_path)
