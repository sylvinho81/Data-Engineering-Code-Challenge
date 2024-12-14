from pyspark.sql.types import (
    StructType,
    StructField,
    StringType, TimestampType
)

SCHEMA_PRODUCTS_SILVER_DATA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)