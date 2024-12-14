from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)


SCHEMA_STORES_DATA = StructType(
    [
        StructField("store_id", StringType(), False),
        StructField("store_name", StringType(), False),
        StructField("location", StringType(), False),
    ]
)
