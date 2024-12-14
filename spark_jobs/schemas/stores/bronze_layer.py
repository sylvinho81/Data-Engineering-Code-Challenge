from pyspark.sql.types import (
    StructType,
    StructField,
    StringType, ArrayType, BooleanType, TimestampType,
)


SCHEMA_STORES_BRONZE_DATA = StructType(
    [
        StructField("store_id", StringType(), True),
        StructField("store_name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("is_invalid", ArrayType(BooleanType()), True),
        StructField("reason", ArrayType(StringType()), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)
