from pyspark.sql.types import (
    StructType,
    StructField,
    StringType, TimestampType,
)


SCHEMA_STORES_SILVER_DATA = StructType(
    [
        StructField("store_id", StringType(), False),
        StructField("store_name", StringType(), False),
        StructField("location", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)
