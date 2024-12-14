from pyspark.sql import SparkSession


def _initiate_spark_session(app_name: str = "Pyspark Sales Transactions") -> SparkSession:
    """
    Initialize and configure a SparkSession for a PySpark application with Delta Lake support.

    Args:
        app_name (str): The name of the Spark application. Defaults to "Pyspark Sales Transactions".

    Returns:
        SparkSession: A configured SparkSession object.
    """

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
