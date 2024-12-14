import sys
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from spark_jobs.schemas.products.bronze_layer import SCHEMA_PRODUCTS_BRONZE_DATA
from spark_jobs.schemas.products.landing_layer import SCHEMA_PRODUCTS_RAW_DATA
from spark_jobs.schemas.sales.bronze_layer import SCHEMA_SALES_BRONZE_DATA
from spark_jobs.schemas.sales.landing_layer import SCHEMA_SALES_RAW_DATA
from spark_jobs.schemas.stores.bronze_layer import SCHEMA_STORES_BRONZE_DATA
from spark_jobs.schemas.stores.landing_layer import SCHEMA_STORES_RAW_DATA
from spark_jobs.utils.dataframes import load_csv, _add_dates
from spark_jobs.utils.delta_lake import _create_delta_lake_table, _store_delta_table_by_merge
from spark_jobs.utils.spark_helper import _initiate_spark_session
from spark_jobs.validations.utils import validate_data


def _validate_data(spark: SparkSession, source_path: str, destination_path: str, config_path: str,
                   schema_landing: StructType, schema_bronze: StructType,
                   columns_match: List[str], columns_update: List[str]) -> None:
    df = load_csv(spark=spark, path=source_path, schema=schema_landing)
    validated_df = validate_data(input_df=df, config_path=config_path)
    validated_df = _add_dates(df=validated_df)
    _create_delta_lake_table(
        spark_session=spark, delta_path=destination_path, schema_table=schema_bronze
    )
    _store_delta_table_by_merge(
        df=validated_df,
        delta_path=destination_path,
        spark_session=spark,
        columns_match=columns_match,
        columns_update=columns_update,
        schema_table=schema_bronze,
    )


def main():
    source_path = sys.argv[1]
    destination_bronze_path = sys.argv[2]
    destination_silver_path = sys.argv[3]

    spark = _initiate_spark_session(app_name="Data Preparation Job")

    _validate_data(spark=spark,
                   source_path=f"{source_path}/products_uuid.csv",
                   destination_path=f"{destination_bronze_path}/products",
                   config_path="spark_jobs/validations/products/validations.yaml",
                   schema_landing=SCHEMA_PRODUCTS_RAW_DATA,
                   schema_bronze=SCHEMA_PRODUCTS_BRONZE_DATA,
                   columns_match=["product_id"],
                   columns_update=["product_name", "category"])

    _validate_data(spark=spark,
                   source_path=f"{source_path}/stores_uuid.csv",
                   destination_path=f"{destination_bronze_path}/stores",
                   config_path="spark_jobs/validations/stores/validations.yaml",
                   schema_landing=SCHEMA_STORES_RAW_DATA,
                   schema_bronze=SCHEMA_STORES_BRONZE_DATA,
                   columns_match=["store_id"],
                   columns_update=["store_name", "location"])

    _validate_data(spark=spark,
                   source_path=f"{source_path}/sales_uuid.csv",
                   destination_path=f"{destination_bronze_path}/sales",
                   config_path="spark_jobs/validations/sales/validations.yaml",
                   schema_landing=SCHEMA_SALES_RAW_DATA,
                   schema_bronze=SCHEMA_SALES_BRONZE_DATA,
                   columns_match=["transaction_id", "store_id", "product_id"],
                   columns_update=["quantity", "transaction_date", "price"])


if __name__ == "__main__":
    main()
