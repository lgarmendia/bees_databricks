"""Module to load and process data from the bronze layer to the silver layer."""

import logging
import os
import sys
from datetime import datetime

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import input_file_name, lit, max

sys.path.append("/opt/airflow/breweries_use_case/utils")
import render

logging.basicConfig(level=logging.INFO)


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

# Get bronze and silver locations from configuration
BRONZE_LOCATION = render.get_yaml_value("bronze_location")
SILVER_LOCATION = render.get_yaml_value("silver_location")

# Create Spark session
spark = configure_spark_with_delta_pip(builder).getOrCreate()


def get_max_processed_date(delta_path: str) -> str:
    """Retrieve the most recent processing date from a Delta location.

    :param delta_path: Path to the Delta table where the max date is retrieved.
    :return: The latest processing date as a string in 'YYYYMMDD' format.
    """
    try:
        df = spark.read.load(delta_path)
        return df.select(max(df.created_on)).collect()[0][0].strftime("%Y%m%d")
    except Exception:
        return "19000101" 


def list_bronze_files(bronze_path: str, max_processed_date: str) -> list:
    """List all JSON files in the bronze path that have been updated since the max processed date.

    :param bronze_path: The directory of the bronze files.
    :param max_processed_date: The latest processed date from the target (silver) location.
    :return: A list of file paths to be processed.
    """
    files_to_process = []
    for path in os.listdir(bronze_path):
        # Check if the current path is a file
        if (
                os.path.isfile(os.path.join(bronze_path, path))
                and path.endswith(".json")
                and path.split("_")[1].split(".")[0] > max_processed_date
        ):
            # Properly concatenate the file path and normalize the slashes
            file_path = os.path.join(bronze_path, path).replace("\\", "")
            files_to_process.append(file_path)
    return files_to_process if files_to_process else files_to_process.append(bronze_path)


def load_data(src_location: str, tgt_location: str, rerun: bool = False) -> None:
    """Load data from the bronze layer and write it to the silver layer.

    If the rerun flag is set to True, all files from the bronze location will be processed,
    regardless of the last processed date.

    :param src_location: Path to the source (bronze) data location.
    :param tgt_location: Path to the target (silver) data location.
    :param rerun: If True, process all files from the bronze location.
    """
    list_to_process = []

    # Determine files to process based on the max processed date
    if list_bronze_files(src_location, get_max_processed_date(tgt_location)) and not rerun:
        # Log the list of files to be processed
        logging.info(
            "LIST OF FILES TO BE PROCESSED: %s",
            list_bronze_files(src_location, get_max_processed_date(tgt_location)),
        )
        list_to_process = list_bronze_files(src_location, get_max_processed_date(tgt_location))
    elif rerun:
        list_to_process.append(src_location)
    else:
        logging.info("No files to process.")

    if list_to_process:
        try:
            # Read data from the bronze layer
            df = spark.read.option("multiline", "true").json(list_to_process)

            # Add 'created_on' and 'file_path' columns to the data
            df = df.withColumn("created_on", lit(datetime.now())).withColumn(
                "file_path", input_file_name()
            )

            # Write data to the silver layer, partitioned by country and state
            df.write.format("delta").mode("overwrite").option("mergeSchema", "True").partitionBy(
                "country", "state"
            ).save(tgt_location)

            logging.info("Process finished successfully!")
        except Exception as e:
            logging.error("Error during processing: %s", e)


# Run the load_data function with the configured paths
load_data(BRONZE_LOCATION, SILVER_LOCATION + "silver_breweries")
