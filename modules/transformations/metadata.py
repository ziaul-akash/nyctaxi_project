from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp


def add_processed_timestamp(df: DataFrame) -> DataFrame:
    """
    Adds a processed timestamp column to datafarame with current_timestamp
    """
    return df.withColumn("processed_timestamp", current_timestamp())