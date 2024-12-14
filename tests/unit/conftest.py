import pytest


@pytest.fixture
def fake_sales_data(spark):
    data = [("6c8c4850-13be-454f-8dce-30054f2330c4",
             "682bda8b-ca30-4a13-b402-98756c99ca31",
             "6a8c7b88-109d-4c16-b8a4-dd746a85cf9c",
             3,
             "2024-07-04",
             1.31)]
    columns = ["transaction_id", "store_id", "product_id", "quantity", "transaction_date", "price"]

    return spark.createDataFrame(data, columns)


@pytest.fixture
def fake_invalid_sales_data(spark):
    data = [("6c8c4850-13be-454f-8dce-30054f2330c4",
             "682bda8b-ca30-4a13-b402-98756c99ca31",
             "6a8c7b88-109d-4c16-b8a4-dd746a85cf9c",
             3,
             "February 25, 2024",
             1.31)]
    columns = ["transaction_id", "store_id", "product_id", "quantity", "transaction_date", "price"]

    return spark.createDataFrame(data, columns)

