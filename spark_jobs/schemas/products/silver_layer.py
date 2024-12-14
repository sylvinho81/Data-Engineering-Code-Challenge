from pyspark.sql.types import (
    StructType,
    StructField,
    StringType, TimestampType
)

SCHEMA_PRODUCTS_SILVER_DATA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)