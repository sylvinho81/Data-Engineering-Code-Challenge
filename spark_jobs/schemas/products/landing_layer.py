from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

SCHEMA_PRODUCTS_RAW_DATA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
    ]
)
