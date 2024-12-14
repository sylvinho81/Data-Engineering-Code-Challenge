from pyspark.sql import DataFrame
from pyspark.sql.functions import array

from spark_jobs.validations.data_validation import DataValidation


def validate_data(input_df: DataFrame, config_path: str) -> DataFrame:
    input_df = input_df.withColumns({
        'is_invalid': array(),
        'reason': array(),
    })
    data_validation = DataValidation(input_df=input_df, config_path=config_path)
    validated_df = data_validation.run_validations()

    return validated_df
