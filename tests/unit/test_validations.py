from pyspark.sql.functions import array_contains, col, lit, array
from chispa.dataframe_comparer import assert_df_equality

from spark_jobs.validations.utils import validate_data


def test_validate_right_sales_data(spark, fake_sales_data):
    sales_df = fake_sales_data
    validated_df = validate_data(input_df=sales_df, config_path="spark_jobs/validations/sales/validations.yaml")

    invalid_rows_df = validated_df.filter(array_contains(col("is_invalid"), True))

    empty_df = spark.createDataFrame([], invalid_rows_df.schema)

    assert_df_equality(invalid_rows_df, empty_df, ignore_nullable=True)


def test_validate_wrong_sales_data(spark, fake_invalid_sales_data):
    invalid_sales_df = fake_invalid_sales_data
    validated_df = validate_data(input_df=invalid_sales_df, config_path="spark_jobs/validations/sales/validations.yaml")

    invalid_rows_df = validated_df.filter(array_contains(col("is_invalid"), True))

    final_invalid_df = invalid_rows_df.withColumns({
        "is_invalid": array(lit(False), lit(True)),
        "reason": array(lit(''), lit('transaction_date format should be yyyy-MM-dd'))
    })

    assert_df_equality(invalid_rows_df, final_invalid_df, ignore_nullable=True)
