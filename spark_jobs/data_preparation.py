import sys

from pyspark.sql import SparkSession

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


def _process_products(spark: SparkSession, source_path: str, destination_path: str) -> None:
    products_df = load_csv(spark=spark, path=f"{source_path}/products_uuid.csv", schema=SCHEMA_PRODUCTS_RAW_DATA)
    validated_products_df = validate_data(input_df=products_df,
                                          config_path="spark_jobs/validations/products/validations.yaml")
    validated_products_df = _add_dates(df=validated_products_df)
    products_path = f"{destination_path}/products"

    _create_delta_lake_table(
        spark_session=spark, delta_path=products_path, schema_table=SCHEMA_PRODUCTS_BRONZE_DATA
    )
    _store_delta_table_by_merge(
        df=validated_products_df,
        delta_path=products_path,
        spark_session=spark,
        columns_match=["product_id"],
        columns_update=["product_name", "category"],
        schema_table=SCHEMA_PRODUCTS_BRONZE_DATA,
    )


def _process_stores(spark: SparkSession, source_path: str, destination_path: str) -> None:
    stores_df = load_csv(spark=spark, path=f"{source_path}/stores_uuid.csv", schema=SCHEMA_STORES_RAW_DATA)
    validated_stores_df = validate_data(input_df=stores_df,
                                          config_path="spark_jobs/validations/stores/validations.yaml")
    validated_stores_df = _add_dates(df=validated_stores_df)

    stores_path = f"{destination_path}/stores"

    _create_delta_lake_table(
        spark_session=spark, delta_path=stores_path, schema_table=SCHEMA_STORES_BRONZE_DATA
    )
    _store_delta_table_by_merge(
        df=validated_stores_df,
        delta_path=stores_path,
        spark_session=spark,
        columns_match=["store_id"],
        columns_update=["store_name", "location"],
        schema_table=SCHEMA_STORES_BRONZE_DATA,
    )


def _process_sales(spark: SparkSession, source_path: str, destination_path: str) -> None:
    sales_df = load_csv(spark=spark, path=f"{source_path}/sales_uuid.csv", schema=SCHEMA_SALES_RAW_DATA)
    validated_sales_df = validate_data(input_df=sales_df,
                                          config_path="spark_jobs/validations/sales/validations.yaml")
    validated_sales_df = _add_dates(df=validated_sales_df)
    sales_path = f"{destination_path}/sales"

    _create_delta_lake_table(
        spark_session=spark, delta_path=sales_path, schema_table=SCHEMA_SALES_BRONZE_DATA
    )

    _store_delta_table_by_merge(
        df=validated_sales_df,
        delta_path=sales_path,
        spark_session=spark,
        columns_match=["transaction_id", "store_id", "product_id"],
        columns_update=["quantity", "transaction_date", "price"],
        schema_table=SCHEMA_SALES_BRONZE_DATA,
    )


def main():
    source_path = sys.argv[1]
    destination_path = sys.argv[2]

    spark = _initiate_spark_session(app_name="Data Preparation Job")

    _process_products(spark=spark, source_path=source_path, destination_path=destination_path)
    _process_stores(spark=spark, source_path=source_path, destination_path=destination_path)
    _process_sales(spark=spark, source_path=source_path, destination_path=destination_path)


if __name__ == "__main__":
    main()
