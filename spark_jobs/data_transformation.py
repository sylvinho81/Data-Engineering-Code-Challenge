import sys
import logging

from spark_jobs.utils.spark_helper import _initiate_spark_session

# Setting up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    source_path = sys.argv[1]
    destination_gold_path = sys.argv[2]

    logger.info(f"Silver path: {source_path}")
    logger.info(f"Gold path: {destination_gold_path}")

    spark = _initiate_spark_session(app_name="Data Transformation Job")


if __name__ == "__main__":
    main()
