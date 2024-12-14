import pytest

from spark_jobs.utils.udfs import categorize_products
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from chispa.dataframe_comparer import assert_df_equality


@pytest.mark.parametrize(
    "input_data, expected_output",
    [
        ([(1, 10), (2, 50), (3, 150)], [(1, 10, "Low"), (2, 50, "Medium"), (3, 150, "High")]),
        ([(1, 5), (2, 25), (3, 120)], [(1, 5, "Low"), (2, 25, "Medium"), (3, 120, "High")])
    ],
    ids=["Price Categorization - Low, Medium, High",
         "Price Categorization - Low, Medium, High with Different Prices",
         ]
)
def test_categorize_products(spark, input_data, expected_output):
    input_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("price", IntegerType(), True)
    ])

    expected_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("price", IntegerType(), True),
        StructField("price_category", StringType(), True)
    ])

    df = spark.createDataFrame(input_data, input_schema)
    expected_df = spark.createDataFrame(expected_output, expected_schema)

    result_df = categorize_products(df)
    assert_df_equality(result_df, expected_df, ignore_nullable=True)
