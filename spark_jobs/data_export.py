import sys
import logging

from pyspark.sql import SparkSession

from spark_jobs.utils.delta_lake import load_delta_lake_table, _store_delta_table_by_overwrite
from spark_jobs.utils.spark_helper import _initiate_spark_session

# Setting up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def export_enriched_data(spark: SparkSession, source_path: str, destination_path: str) -> None:
    """
    Export the enriched data from a Delta table to a destination path, overwriting the existing data.

    This function loads the enriched data from the source Delta table, processes it, and stores the result in the
    destination path while partitioning the data based on specified columns.

    Args:
        spark (SparkSession): The SparkSession used to interact with Spark.
        source_path (str): The path to the source Delta table that contains the enriched data.
        destination_path (str): The path where the enriched data will be stored.

    Returns:
        None: This function does not return anything. It performs the data export operation.

    Notes:
        - The function currently partitions the data by `category` and `transaction_date`, but it is intended to
          partition by `transaction year`, `transaction month`, and `transaction day` for better performance and
          organization.
        - The data is stored in the Delta format and is overwritten if the destination already exists.
    """

    logger.info(f"Exporting enriched data to {destination_path}")

    enriched_data_df = load_delta_lake_table(spark=spark, delta_table_path=f"{source_path}/enriched_data_tmp")

    # TODO: I would partition by transaction year, transaction month, transaction day instead by transaction date
    _store_delta_table_by_overwrite(df=enriched_data_df, delta_path=f"{destination_path}/enriched_data",
                                    partition_columns=["category", "transaction_date"])


def save_revenue_insights(spark: SparkSession, source_path: str, destination_path: str) -> None:
    """
    Export the revenue insights DataFrame to CSV format at the specified destination path.

    Args:
        spark (SparkSession): The SparkSession used to interact with Spark.
        source_path (str): The path to the source Delta table that contains the revenue insights data.
        destination_path (str): The path where the revenue insights CSV data will be stored.

    Returns:
        None: This function performs the export operation without returning anything.
    """

    logger.info(f"Exporting revenue insights to {destination_path}")

    revenue_insights_df = load_delta_lake_table(spark=spark, delta_table_path=f"{source_path}/sales_total_revenue_tmp")

    revenue_insights_df.write.csv(f"{destination_path}/sales_total_revenue", mode="overwrite", header=True)


def main():
    source_gold_path = sys.argv[1]
    destination_gold_path = sys.argv[2]

    logger.info(f"Source path: {source_gold_path}")
    logger.info(f"Gold path: {destination_gold_path}")

    spark = _initiate_spark_session(app_name="Data Transformation Job")

    export_enriched_data(spark=spark, source_path=source_gold_path, destination_path=destination_gold_path)

    save_revenue_insights(spark=spark, source_path=source_gold_path, destination_path=destination_gold_path)


if __name__ == "__main__":
    main()
