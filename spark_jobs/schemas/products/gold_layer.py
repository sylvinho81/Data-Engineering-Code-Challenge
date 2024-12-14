from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


SCHEMA_PRODUCTS_DATA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), False),
    ]
)