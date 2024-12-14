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
    if partition_columns:
        df.write.mode("append").partitionBy(*partition_columns).format("delta").save(delta_path)
    else:
        df.write.mode("append").format("delta").save(delta_path)


def _store_delta_table_by_merge(
    df: DataFrame,
    delta_path: str,
    spark_session: SparkSession,
    columns_match: List[str],
    columns_update: List[str],
    schema_table: StructType,
    partition_columns: Optional[List[str]] = None,
) -> None:
    try:
        delta_table = DeltaTable.forPath(spark_session, delta_path)
        match_condition = " AND ".join(f"dest.{column} = updates.{column}" for column in columns_match)

        # Create a condition to check if any of the update columns have changed
        update_condition = " OR ".join(f"dest.{column} != updates.{column}" for column in columns_update)
        columns_update.append("updated_at")

        delta_table.alias("dest").merge(df.alias("updates"), match_condition).whenMatchedUpdate(
            condition=update_condition, set={column: f"updates.{column}" for column in columns_update}
        ).whenNotMatchedInsertAll().execute()
    except AnalysisException:
        _create_delta_lake_table(
            spark_session=spark_session,
            delta_path=delta_path,
            schema_table=schema_table,
            partition_columns=partition_columns,
        )
        _store_delta_table_by_append(df=df, delta_path=delta_path, partition_columns=partition_columns)


def load_delta_lake_table(spark: SparkSession, delta_table_path: str) -> DataFrame:
    """
    Load a delta lake table from delta_table_path

    Parameters:
    spark (SparkSession): The Spark session.
    delta_table_path (str): Path to the delta lake table.

    Returns: The DataFrame
    """

    return spark.read.format("delta").load(delta_table_path)
