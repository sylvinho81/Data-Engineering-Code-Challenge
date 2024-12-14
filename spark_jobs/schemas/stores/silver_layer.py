from pyspark.sql.types import (
    StructType,
    StructField,
    StringType, TimestampType,
)


SCHEMA_STORES_SILVER_DATA = StructType(
    [
        StructField("store_id", StringType(), True),
        StructField("store_name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)
