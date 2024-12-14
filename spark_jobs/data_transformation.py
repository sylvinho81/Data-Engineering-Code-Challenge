import sys
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, sum, year, month, dayofmonth

from spark_jobs.utils.delta_lake import load_delta_lake_table, _store_delta_table_by_overwrite
from spark_jobs.utils.spark_helper import _initiate_spark_session

# Setting up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def sales_aggregation(spark: SparkSession, source_silver_path: str, destination_path: str) -> DataFrame:
    """
    Aggregates the total revenue by store and product category, and stores the result in a Delta Lake table.

    Args:
        spark (SparkSession): The Spark session object used for DataFrame operations.
        source_silver_path (str): The path to the source Silver Delta tables (sales, products).
        destination_path (str): The path where the sales aggregation results should be stored.

    Returns:
        DataFrame: The aggregated sales DataFrame containing total revenue by store and category.
    """

    logger.info(f"Sales aggregation started for {source_silver_path}")

    sales_silver_df = (load_delta_lake_table(spark=spark, delta_table_path=f"{source_silver_path}/sales_silver").
                       select(col("store_id"), col("price"), col("product_id")))
    products_silver_df = (load_delta_lake_table(spark=spark, delta_table_path=f"{source_silver_path}/products_silver").
                          select(col("product_id"), col("category")))

    sales_products_df = sales_silver_df.join(products_silver_df, on="product_id")

    sales_products_df.printSchema()

    sales_total_revenue_df = (
        sales_products_df.groupBy(col("store_id"), col("category")).agg(sum("price").alias("total_revenue"))
        .orderBy("store_id")
    )

    _store_delta_table_by_overwrite(df=sales_total_revenue_df, delta_path=destination_path)
    #sales_total_revenue_df.write.csv(destination_path, mode="overwrite", header=True)

    return sales_total_revenue_df


def monthly_sales_insights(spark: SparkSession, source_silver_path: str, destination_path: str) -> DataFrame:
    """
    Computes monthly sales insights by aggregating total quantity sold by product category, year, and month.
    The results are saved to a Delta Lake table and returned as a DataFrame.

    Args:
        spark (SparkSession): The Spark session object used for DataFrame operations.
        source_silver_path (str): The path to the source Silver Delta tables (sales, products).
        destination_path (str): The path where the monthly sales insights Delta table should be stored.

    Returns:
        DataFrame: The aggregated monthly sales insights DataFrame containing total quantity sold by
        category, year, and month.
    """

    logger.info(f"Monthly sales insights started for {destination_path}")

    sales_silver_df = (load_delta_lake_table(spark=spark, delta_table_path=f"{source_silver_path}/sales_silver").
                       select(col("transaction_date"), col("quantity"), col("product_id")))
    sales_silver_df = sales_silver_df.withColumns({
        "year": year(col("transaction_date")),
        "month": month(col("transaction_date")),
        "day": dayofmonth(col("transaction_date"))
    })

    products_silver_df = (load_delta_lake_table(spark=spark, delta_table_path=f"{source_silver_path}/products_silver").
                          select(col("product_id"), col("category")))

    sales_products_df = sales_silver_df.join(products_silver_df, on="product_id")

    sales_products_df.printSchema()

    monthly_sales_insights_df = (
        sales_products_df.groupBy(
            col("year"),
            col("month"),
            col("category")
        )
        .agg(sum("quantity").alias("total_quantity_sold"))
        .orderBy("category", "year", "month")
    )

    monthly_sales_insights_df.show()
    _store_delta_table_by_overwrite(df=monthly_sales_insights_df, delta_path=destination_path)

    return monthly_sales_insights_df


def enrich_data(spark: SparkSession, source_silver_path: str, destination_path: str) -> DataFrame:
    """
    Enriches the sales data by joining it with product and store information,
    extracting date-related features, and saving the enriched data to a Delta Lake table.

    Args:
        spark (SparkSession): The Spark session object used for DataFrame operations.
        source_silver_path (str): The path to the source Silver Delta tables (sales, products, stores).
        destination_path (str): The path where the enriched Delta table should be stored.

    Returns:
        DataFrame: The enriched DataFrame containing transaction details with additional date features
        (year, month, day).

    """

    logger.info(f"Enrich data started for {destination_path}")

    sales_silver_df = load_delta_lake_table(spark=spark, delta_table_path=f"{source_silver_path}/sales_silver")
    products_silver_df = load_delta_lake_table(spark=spark, delta_table_path=f"{source_silver_path}/products_silver")
    stores_silver_df = load_delta_lake_table(spark=spark, delta_table_path=f"{source_silver_path}/stores_silver")

    sales_products_df = sales_silver_df.join(products_silver_df, on="product_id")
    enriched_df = sales_products_df.join(stores_silver_df, on="store_id")

    enriched_df = enriched_df.select(
        "transaction_id",
        "store_name",
        "location",
        "product_name",
        "category",
        "quantity",
        "transaction_date",
        "price",
    )
    enriched_df = enriched_df.withColumns({
        "year": year(col("transaction_date")),
        "month": month(col("transaction_date")),
        "day": dayofmonth(col("transaction_date"))
    })

    _store_delta_table_by_overwrite(df=enriched_df, delta_path=destination_path)

    return enriched_df


def main():
    source_silver_path = sys.argv[1]
    destination_gold_path = sys.argv[2]

    logger.info(f"Silver path: {source_silver_path}")
    logger.info(f"Gold path: {destination_gold_path}")

    spark = _initiate_spark_session(app_name="Data Transformation Job")

    sales_aggregation(spark=spark, source_silver_path=source_silver_path,
                      destination_path=f"{destination_gold_path}/sales_total_revenue_tmp")
    monthly_sales_insights(spark=spark, source_silver_path=source_silver_path,
                      destination_path=f"{destination_gold_path}/monthly_sales_insights_tmp")
    enrich_data(spark=spark, source_silver_path=source_silver_path,
                      destination_path=f"{destination_gold_path}/enriched_data_tmp")


if __name__ == "__main__":
    main()
