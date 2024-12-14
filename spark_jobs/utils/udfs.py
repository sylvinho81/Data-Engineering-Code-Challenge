from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType


def categorize_products(df: DataFrame) -> DataFrame:
    """
    Categorizes products based on their price into 'Low', 'Medium', or 'High' categories.
    A new column 'price_category' is added to the DataFrame with the respective category.

    Args:
        df (DataFrame): The input DataFrame containing product data with a 'price' column.

    Returns:
        DataFrame or None: The DataFrame with an additional 'price_category' column if 'price' exists,
                           otherwise None if 'price' column is missing.
    """

    def categorize_price(price):
        if price < 20:
            return "Low"
        elif 20 <= price <= 100:
            return "Medium"
        else:
            return "High"

    categorize_price_udf = udf(categorize_price, StringType())

    categorized_price_df = df.withColumn(
        "price_category", categorize_price_udf(col("price"))
    )

    return categorized_price_df
