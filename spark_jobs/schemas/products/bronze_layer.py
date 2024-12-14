from pyspark.sql.types import (
    StructType,
    StructField,
    StringType, ArrayType, BooleanType, TimestampType
)

SCHEMA_PRODUCTS_BRONZE_DATA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("is_invalid", ArrayType(BooleanType()), True),
        StructField("reason", ArrayType(StringType()), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)