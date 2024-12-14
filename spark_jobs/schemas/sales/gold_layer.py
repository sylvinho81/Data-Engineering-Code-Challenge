from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    StringType,
    LongType,
    DoubleType,
)


SCHEMA_SALES_DATA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("store_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", LongType(), False),
        StructField("transaction_date", DateType(), False),
        StructField("price", DoubleType(), False),
    ]
)
