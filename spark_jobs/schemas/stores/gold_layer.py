from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    StringType,
    LongType,
    DoubleType,
    IntegerType,
    BooleanType,
    DecimalType
)


SCHEMA_STORES_DATA = StructType(
    [
        StructField("store_id", StringType(), False),
        StructField("store_name", StringType(), False),
        StructField("location", StringType(), False),
    ]
)
