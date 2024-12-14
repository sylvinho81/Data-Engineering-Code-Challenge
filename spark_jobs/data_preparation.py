import sys
import logging

from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains
from pyspark.sql.types import StructType

from spark_jobs.schemas.products.bronze_layer import SCHEMA_PRODUCTS_BRONZE_DATA
from spark_jobs.schemas.products.landing_layer import SCHEMA_PRODUCTS_RAW_DATA
from spark_jobs.schemas.products.silver_layer import SCHEMA_PRODUCTS_SILVER_DATA
from spark_jobs.schemas.sales.bronze_layer import SCHEMA_SALES_BRONZE_DATA
from spark_jobs.schemas.sales.landing_layer import SCHEMA_SALES_RAW_DATA
from spark_jobs.schemas.sales.silver_layer import SCHEMA_SALES_SILVER_DATA
from spark_jobs.schemas.stores.bronze_layer import SCHEMA_STORES_BRONZE_DATA
from spark_jobs.schemas.stores.landing_layer import SCHEMA_STORES_RAW_DATA
from spark_jobs.schemas.stores.silver_layer import SCHEMA_STORES_SILVER_DATA
from spark_jobs.utils.dataframes import load_csv, _add_dates
from spark_jobs.utils.delta_lake import _create_delta_lake_table, _store_delta_table_by_merge, load_delta_lake_table
from spark_jobs.utils.spark_helper import _initiate_spark_session
from spark_jobs.validations.utils import validate_data


# Setting up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _validate_data(spark: SparkSession, source_path: str, destination_path: str, config_path: str,
                   schema_landing: StructType, schema_bronze: StructType,
                   columns_match: List[str], columns_update: List[str]) -> None:
    """
    Validates the data from a source path and stores the validated DataFrame in Delta Lake.

    This function loads data from a CSV file, applies data validation using a yaml configuration file,
    and performs necessary transformations (e.g., adding load timestamp). The validated DataFrame is then
    written to a Delta Lake table.

    Args:
        spark (SparkSession): The Spark session used for processing the data.
        source_path (str): The file path to the source CSV data.
        destination_path (str): The path where the validated data will be stored in Delta Lake.
        config_path (str): The path to the yaml configuration file used for data validation.
        schema_landing (StructType): The schema for the landing data.
        schema_bronze (StructType): The schema for the bronze raw data with some metadata in Delta Lake.
        columns_match (List[str]): The columns to match on during the merge operation.
        columns_update (List[str]): The columns to update during the merge operation.

    Returns:
        None
    """
    logger.info(f"Validating data for {source_path}")

    df = load_csv(spark=spark, path=source_path, schema=schema_landing)
    validated_df = validate_data(input_df=df, config_path=config_path)
    validated_df = _add_dates(df=validated_df)

    # Enforce the appropriate data types for all columns.
    _create_delta_lake_table(
        spark_session=spark, delta_path=destination_path, schema_table=schema_bronze
    )
    _store_delta_table_by_merge(
        df=validated_df,
        delta_path=destination_path,
        spark_session=spark,
        columns_match=columns_match,
        columns_update=columns_update,
    )


def _filter_and_deduplicate_data(spark: SparkSession, source_path: str, destination_path: str,
                      columns_for_deduplication: List[str], schema_silver: StructType,
                      columns_match: List[str], columns_update: List[str]) -> None:
    """
    Filters, deduplicates, and stores data into Delta Lake, ensuring that only valid records are retained.

    This function loads a Delta Lake table, filters out invalid data, and identifies any duplicates
    based on the specified columns. It removes duplicate entries and stores the cleaned data into a
    Delta Lake table, merging it with the existing records. Invalid records and duplicates could be
    optionally stored for later analysis.

    Args:
        spark (SparkSession): The Spark session used for processing the data.
        source_path (str): The path to the source Delta Lake table.
        destination_path (str): The path where the filtered and deduplicated data will be stored in Delta Lake.
        columns_for_deduplication (List[str]): The columns used to identify duplicates.
        schema_silver (StructType): The schema for the silver (cleaned) data in Delta Lake.
        columns_match (List[str]): The columns to match on during the merge operation.
        columns_update (List[str]): The columns to update during the merge operation.

    Returns:
        None
    """
    logger.info(f"Filtering and deduplicating data for {source_path}")

    delta_table_df = load_delta_lake_table(spark=spark, delta_table_path=source_path)
    valid_df = delta_table_df.filter(~array_contains("is_invalid", True))

    # TODO: we could store the invalid ones after applying validation in parquet files so Data Team could analyze it
    # invalid_df = delta_table_df.filter(~array_contains("is_invalid", True))

    # TODO: we could store the duplicates in parquet files for later analysis
    # duplicates_df = (
    #    valid_df.groupBy(columns_for_deduplication).agg(count("*").alias("count")).filter(col("count") > 1)
    #)

    valid_without_duplicated_df = valid_df.dropDuplicates(columns_for_deduplication).drop("is_invalid", "reason")

    # Enforce the appropriate data types for all columns.
    _create_delta_lake_table(
        spark_session=spark, delta_path=destination_path, schema_table=schema_silver
    )

    _store_delta_table_by_merge(
        df=valid_without_duplicated_df,
        delta_path=destination_path,
        spark_session=spark,
        columns_match=columns_match,
        columns_update=columns_update
    )


def main():
    source_path = sys.argv[1]
    destination_bronze_path = sys.argv[2]
    destination_silver_path = sys.argv[3]

    logger.info(f"Landing path: {source_path}")
    logger.info(f"Bronze path: {destination_bronze_path}")
    logger.info(f"Silver path: {destination_silver_path}")

    spark = _initiate_spark_session(app_name="Data Preparation Job")

    _validate_data(spark=spark,
                   source_path=f"{source_path}/products_uuid.csv",
                   destination_path=f"{destination_bronze_path}/products_bronze",
                   config_path="spark_jobs/validations/products/validations.yaml",
                   schema_landing=SCHEMA_PRODUCTS_RAW_DATA,
                   schema_bronze=SCHEMA_PRODUCTS_BRONZE_DATA,
                   columns_match=["product_id"],
                   columns_update=["product_name", "category"])

    _filter_and_deduplicate_data(spark=spark,
                                  source_path=f"{destination_bronze_path}/products_bronze",
                                  destination_path=f"{destination_silver_path}/products_silver",
                                  columns_for_deduplication=["product_id"],
                                  schema_silver=SCHEMA_PRODUCTS_SILVER_DATA,
                                  columns_match=["product_id"],
                                  columns_update=["product_name", "category"])

    _validate_data(spark=spark,
                   source_path=f"{source_path}/stores_uuid.csv",
                   destination_path=f"{destination_bronze_path}/stores_bronze",
                   config_path="spark_jobs/validations/stores/validations.yaml",
                   schema_landing=SCHEMA_STORES_RAW_DATA,
                   schema_bronze=SCHEMA_STORES_BRONZE_DATA,
                   columns_match=["store_id"],
                   columns_update=["store_name", "location"])

    _filter_and_deduplicate_data(spark=spark,
                                  source_path=f"{destination_bronze_path}/stores_bronze",
                                  destination_path=f"{destination_silver_path}/stores_silver",
                                  columns_for_deduplication=["store_id"],
                                  schema_silver=SCHEMA_STORES_SILVER_DATA,
                                  columns_match=["store_id"],
                                  columns_update=["store_name", "location"])

    _validate_data(spark=spark,
                   source_path=f"{source_path}/sales_uuid.csv",
                   destination_path=f"{destination_bronze_path}/sales_bronze",
                   config_path="spark_jobs/validations/sales/validations.yaml",
                   schema_landing=SCHEMA_SALES_RAW_DATA,
                   schema_bronze=SCHEMA_SALES_BRONZE_DATA,
                   columns_match=["transaction_id", "store_id", "product_id"],
                   columns_update=["quantity", "transaction_date", "price"])

    _filter_and_deduplicate_data(spark=spark,
                                 source_path=f"{destination_bronze_path}/sales_bronze",
                                 destination_path=f"{destination_silver_path}/sales_silver",
                                 columns_for_deduplication=["transaction_id", "store_id", "product_id"],
                                 schema_silver=SCHEMA_SALES_SILVER_DATA,
                                 columns_match=["transaction_id", "store_id", "product_id"],
                                 columns_update=["quantity", "transaction_date", "price"])


if __name__ == "__main__":
    main()
