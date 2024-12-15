import pytest

from datetime import datetime

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import StructType, StructField, DateType

from spark_jobs.utils.dates import transform_dates


@pytest.mark.parametrize(
    "input_data,expected_data,column_name",
    [
        ([("2024-07-04",)], [(datetime.strptime("2024-07-04", "%Y-%m-%d").date(),)], "transaction_date"),
        ([("2024/11/04",)], [(datetime.strptime("2024-11-04", "%Y-%m-%d").date(),)], "transaction_date"),
        ([("05/15/2024",)], [(datetime.strptime("2024-05-15", "%Y-%m-%d").date(),)], "transaction_date"),
        ([("20-11-2024",)], [(datetime.strptime("2024-11-20", "%Y-%m-%d").date(),)], "transaction_date"),
        ([("February 25, 2024",)], [(datetime.strptime("2024-02-25", "%Y-%m-%d").date(),)], "transaction_date"),
        ([("25.12.2024",)], [(None,)], "transaction_date"),
    ],
    ids=[
        "already_target_format",
        "slash_format",
        "us_format",
        "hyphen_format",
        "full_month_format",
        "invalid_format",
    ]
)
def test_transform_dates(spark, input_data, expected_data, column_name):
    expected_schema = StructType([
        StructField(column_name, DateType(), True)
    ])

    input_df = spark.createDataFrame(input_data, [column_name])
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    result_df = transform_dates(input_df, column_name)
    assert_df_equality(result_df, expected_df, ignore_row_order=True, ignore_column_order=True)
