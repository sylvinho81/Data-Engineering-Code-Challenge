from pyspark.sql.types import (
    StructType,
    StructField,
    StringType, TimestampType, IntegerType, DateType, DoubleType,
)


SCHEMA_SALES_SILVER_DATA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("store_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("transaction_date", DateType(), False),
        StructField("price", DoubleType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)