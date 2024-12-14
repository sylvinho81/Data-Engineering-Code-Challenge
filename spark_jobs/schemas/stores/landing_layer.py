from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)


SCHEMA_STORES_RAW_DATA = StructType(
    [
        StructField("store_id", StringType(), True),
        StructField("store_name", StringType(), True),
        StructField("location", StringType(), True),
    ]
)
