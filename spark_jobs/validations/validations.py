from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, lit, to_date, col, array, array_union, coalesce, array_contains


class Validation(ABC):
    """
    Abstract base class for data validation.

    Attributes:
        column (str): The name of the column to validate.
        add_info (dict): Additional information or parameters for the validation.
    """

    def __init__(self, column: str, add_info: dict = {}):
        self.column = column
        self.add_info = add_info

    @abstractmethod
    def validate(self, input_df: DataFrame) -> DataFrame:
        pass


class NotNullValidation(Validation):
    """
    Validation class to ensure a column has no null values.
    """

    def __init__(self, column: str, add_info: dict = {}):
        super().__init__(column, add_info)

    def validate(self, input_df: DataFrame) -> DataFrame:
        """
        Perform the not-null validation.

        Args:
            input_df (DataFrame): The input DataFrame to validate.

        Returns:
            DataFrame: The validated DataFrame with updated `is_invalid` and `reason` columns.
        """

        df = input_df.withColumn(
            "is_invalid",
            array_union(
                coalesce(col("is_invalid"), array()),
                array(when(input_df[self.column].isNull(), lit(True)).otherwise(lit(False)))
            )
        ).withColumn(
            "reason",
            array_union(
                coalesce(col("reason"), array()),
                array(when(input_df[self.column].isNull(), lit(f"{self.column} cannot be null")).otherwise(lit("")))
            )
        )
        return df


class ValuesToBeBetweenValidation(Validation):
    """
    Validation class to ensure column values are within a specified range.
    """

    def __init__(self, column: str, add_info: dict = {}):
        super().__init__(column, add_info)

    def validate(self, input_df: DataFrame) -> DataFrame:
        """
        Perform the range validation.

        Args:
            input_df (DataFrame): The input DataFrame to validate.

        Returns:
            DataFrame: The validated DataFrame with updated `is_invalid` and `reason` columns.

        Raises:
            ValueError: If `min` or `max` values are not provided in `add_info`.
        """

        min_value = self.add_info.get("min")
        max_value = self.add_info.get("max")

        if min_value is None or max_value is None:
            raise ValueError("Both 'min' and 'max' must be provided in add_info.")

        min_value = int(min_value)
        max_value = int(max_value)

        df = input_df.withColumn(
            "is_invalid",
            array_union(
                coalesce(col("is_invalid"), array()),
                array(when(~((input_df[self.column] >= lit(min_value)) & (input_df[self.column] <= lit(max_value))), lit(True))
                      .otherwise(lit(False)))
            )
        ).withColumn(
            "reason",
            array_union(
                coalesce(col("reason"), array()),
                array(when(~((input_df[self.column] >= lit(min_value)) & (input_df[self.column] <= lit(max_value))),
                           lit(f"{self.column} must be between {min_value} and {max_value}"))
                      .otherwise(lit("")))
            )
        )
        return df


class DateFormatValidation(Validation):
    """
    Validation class to ensure a column adheres to a specified date format.
    """

    def __init__(self, column: str, add_info: dict = {"date_format": "yyyy-MM-dd"}):
        super().__init__(column, add_info)
        self.date_format = self.add_info.get("date_format")

    def validate(self, input_df: DataFrame) -> DataFrame:
        """
        Perform the date format validation.

        Args:
            input_df (DataFrame): The input DataFrame to validate.

        Returns:
            DataFrame: The validated DataFrame with updated `is_invalid` and `reason` columns.
        """

        formatted_df = input_df.withColumn(
            "formatted_date",
            to_date(col(self.column), self.date_format)
        )

        df = formatted_df.withColumn(
            "is_invalid",
            array_union(
                coalesce(col("is_invalid"), array()),
                array(when(col("formatted_date").isNull(), lit(True)).otherwise(lit(False)))
            )
        ).withColumn(
            "reason",
            array_union(
                coalesce(col("reason"), array()),
                array(when(col("formatted_date").isNull(),
                           lit(f"{self.column} format should be {self.date_format}"))
                      .otherwise(lit("")))
            )
        )

        df = df.withColumn(
            self.column,
            when(array_contains(col("is_invalid"), lit(True)), col(self.column)).otherwise(col("formatted_date"))
        )
        df = df.drop("formatted_date")

        return df
