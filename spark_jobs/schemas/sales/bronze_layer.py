from pyspark.sql.types import (
    StructType,
    StructField,
    StringType, ArrayType, BooleanType, TimestampType,
)


SCHEMA_SALES_BRONZE_DATA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("price", StringType(), True),
        StructField("is_invalid", ArrayType(BooleanType()), True),
        StructField("reason", ArrayType(StringType()), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)