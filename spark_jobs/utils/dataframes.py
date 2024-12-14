from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import to_timestamp, lit
from pyspark.sql.types import StructType


def load_csv(spark: SparkSession, path: str, schema: StructType, header: bool = True, separator: str = ",") -> (
        DataFrame):

    df = spark.read.format("csv").option("header", header).option("sep", separator).schema(schema=schema).load(path)

    return df


def _current_utc_timestamp() -> str:
    current_utc_time = datetime.utcnow().isoformat()
    return current_utc_time


def _add_dates(df: DataFrame) -> DataFrame:
    current_utc_time = _current_utc_timestamp()
    value_timestamp = to_timestamp(lit(current_utc_time))
    return df.withColumns({
        "created_at": value_timestamp,
        "updated_at": value_timestamp
    })
