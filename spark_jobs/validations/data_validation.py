import yaml
from pyspark.sql import DataFrame
from spark_jobs.validations.validations import (NotNullValidation,  # noqa: F401
                                                ValuesToBeBetweenValidation,  # noqa: F401
                                                DateFormatValidation,  # noqa: F401
                                                )


class DataValidation:
    """
    Class to perform data validation on a PySpark DataFrame based on rules defined in a YAML configuration file.

    Attributes:
        input_df (DataFrame): The input DataFrame to validate.
        config_path (str): The path to the YAML configuration file containing validation rules.
    """

    def __init__(self, input_df: DataFrame, config_path: str):
        self.input_df = input_df
        self.config_path = config_path

    def rule_mapping(self, rule: str):
        """
        Map rule names from the YAML file to their corresponding validation class names.

        Args:
            rule (str): The name of the rule defined in the YAML file.

        Returns:
            str: The corresponding validation class name.
        """

        return {
            "check_if_not_null": "NotNullValidation",
            "check_if_values_between": "ValuesToBeBetweenValidation",
            "check_valid_date": "DateFormatValidation",
        }[rule]

    def _get_validation(self, rule_name: str):
        class_name = self.rule_mapping(rule_name)
        class_obj = globals()[class_name]
        return class_obj

    def read_config(self):
        with open(self.config_path) as f:
            rules = yaml.load(f, Loader=yaml.FullLoader)
        return rules["validations"]

    def run_validations(self):
        """
        Run validation rules defined in the configuration file on the input DataFrame.

        Returns:
            DataFrame: A DataFrame after applying all validation rules.

        Raises:
            KeyError: If a rule in the configuration file does not have a corresponding validation class.
        """

        raw_columns = self.input_df.columns
        config = self.read_config()
        df_validation = self.input_df
        for column in raw_columns:
            validation = config.get(column, None)
            if validation is None:
                continue
            for rule in validation.get("rules"):
                validation_class = self._get_validation(rule["rule_name"])
                validation_instance = validation_class(
                    column,
                    rule["add_info"]
                )
                df_validation = validation_instance.validate(input_df=df_validation)

        return df_validation
