import pytest

from spark_jobs.utils.udfs import categorize_products
from pyspark.sql.types import StructType, StructField, IntegerType


@pytest.mark.parametrize(
    "input_data, expected_output",
    [
        ([(1, 10), (2, 50), (3, 150)], ["Low", "Medium", "High"]),
        ([(1, 5), (2, 25), (3, 120)], ["Low", "Medium", "High"])
    ],
    ids=["Price Categorization - Low, Medium, High",
         "Price Categorization - Low, Medium, High with Different Prices",
         ]
)
def test_categorize_products(spark, input_data, expected_output):
    # Define schema explicitly
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("price", IntegerType(), True)
    ])

    # Create DataFrame with explicit schema
    df = spark.createDataFrame(input_data, schema)

    result = categorize_products(df)
    result_data = result.select("price_category").rdd.flatMap(lambda x: x).collect()
    assert result_data == expected_output

