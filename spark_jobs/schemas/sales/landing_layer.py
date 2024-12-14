from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)


SCHEMA_SALES_RAW_DATA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("price", StringType(), True),
    ]
)
