from pyspark.sql.types import (
    StructType,
    StructField,
    StringType, TimestampType,
)


SCHEMA_SALES_SILVER_DATA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("price", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)