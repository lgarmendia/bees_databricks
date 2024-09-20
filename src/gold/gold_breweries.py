"""Module for loading and processing data from the silver layer to the gold layer."""

import logging
import os
import sys

sys.path.append("/opt/airflow/breweries_use_case/utils")

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import lit, max, col
import render

# Set Python executable for PySpark
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Setup a builder for a Spark session
# Documentation: https://docs.delta.io/latest/quick-start.html#language-python
builder = (
    pyspark.sql.SparkSession.builder.appName("silver_breweries")
    .master("local")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

# Create Spark session
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Load the silver, gold layer and data quality paths from configuration
SILVER_LOCATION = render.get_yaml_value("silver_location")
GOLD_LOCATION = render.get_yaml_value("gold_location")
DATAQUALITY_LOCATION = render.get_yaml_value("quality_location")

logging.basicConfig(level=logging.INFO)


def get_max_processed_file(delta_path: str) -> any:
    """Retrieve the latest processed file from a given Delta table.

    :param delta_path: Path to the Delta table to check for the latest processed file.
    :return: The path of the latest file as a string or None if no files exist.
    """
    try:
        df = spark.read.load(delta_path)
        return df.select(max(df.file_path)).collect()[0][0]
    except Exception:
        return None



def load_data(src_location: str, tgt_location: str, dq_location: str) -> None:
    """Load data from the silver layer and write aggregated results to the gold layer.

    The function filters the data by the most recently processed file from the source (silver),
    performs aggregation by 'brewery_type', 'country', and 'state', and writes the result
    to the target (gold) location.

    :param src_location: The source location (silver layer).
    :param tgt_location: The target location (gold layer).
    :param dq_location:  The target data quality location.
    """

    dq_filtered = spark.read.format("delta").load(dq_location)
    dq_filtered = dq_filtered.filter(col("status") == "error").select("id").distinct()
    
    # Get the latest date from silver and process it.
    if get_max_processed_file(src_location):
        try:
            df = spark.read.format("delta").load(src_location)
            df_filtered = df.join(dq_filtered, on="id", how="left_anti")        
            
            df_filtered \
                .filter(df_filtered.file_path == lit(get_max_processed_file(src_location))) \
                .groupBy("brewery_type", "country", "state") \
                .count() \
                .withColumnRenamed("count", "quantity") \
                .write.format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "True") \
                .partitionBy("country") \
                .save(tgt_location)
                
            logging.info("Process finished successfully!")
        except Exception as e:
            logging.error(f"Error during processing: {e}")
    else:
        logging.info("There's no data to be processed.")


load_data(SILVER_LOCATION + "silver_breweries", GOLD_LOCATION + "gold_breweries", DATAQUALITY_LOCATION )