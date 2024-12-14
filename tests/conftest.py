import pytest

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


@pytest.fixture(scope="session")
def spark():
    packages = [
        'io.delta:delta-core_2.12:2.1.0'
    ]

    builder = (
        SparkSession.builder.master("local[*]")
        .appName("data_challenge")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()

    yield spark

    spark.stop()

