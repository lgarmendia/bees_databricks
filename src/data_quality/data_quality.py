"""Module for data quality."""

import os
import sys
import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, lit, when, concat_ws

# Set Python executable for PySpark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


sys.path.append('/opt/airflow/breweries_use_case/utils')

import render

# Setup a builder for a Spark session
# Documentation: https://docs.delta.io/latest/quick-start.html#language-python
builder = (
    pyspark.sql.SparkSession.builder.appName("silver_breweries")
    .master("local")
    # .config("spark.driver.memory", "4g")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)
# Create Spark session
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Load the silver and data quality paths from configuration
SILVER_LOCATION = render.get_yaml_value("silver_location")
TARGET_LOCATION = render.get_yaml_value("quality_location")

# Read data from the Silver layer in Delta Lake
df = spark.read.load(SILVER_LOCATION)
df = df.drop("created_on", "file_path")


# List of mandatory fields that should not be missing (null)
mandatory_fields = ['id', 'name', 'brewery_type', 'address_1', 'city', 'state_province', 'postal_code', 'country']

# Calculate an expression to count how many mandatory fields are missing (null)
missing_count_expr = sum([col(field).isNull().cast("int") for field in mandatory_fields])

# Create a new column 'field_status' that lists the fields that are missing or invalid
df = df.withColumn("field_status", concat_ws(", ",
    when(col('id').isNull(), lit("id")).otherwise(lit(None)),
    when(col('name').isNull(), lit("name")).otherwise(lit(None)),
    when(col('brewery_type').isNull(), lit("brewery_type")).otherwise(lit(None)),
    when(col('address_1').isNull(), lit("address_1")).otherwise(lit(None)),
    when(col('city').isNull(), lit("city")).otherwise(lit(None)),
    when(col('state_province').isNull(), lit("state_province")).otherwise(lit(None)),
    when(col('postal_code').isNull(), lit("postal_code"))
        .when(~col("postal_code").rlike("^[0-9-]*$"), lit("postal_code")).otherwise(lit(None)),
    when(col('country').isNull(), lit("country")).otherwise(lit(None)),
    when(~col("phone").rlike("^[0-9]*$"), lit("phone")).otherwise(lit(None)),
    when(~col("longitude").cast("double").between(-180, 180), lit("longitude")).otherwise(lit(None)),
    when(~col("latitude").cast("double").between(-90, 90), lit("latitude")).otherwise(lit(None))
))

# Create a new column 'status' that indicates the record status ('error' or 'warning')
df = df.withColumn(
    "status",
    when(missing_count_expr > 0, lit("error")) 
    .when((col("field_status") != "") & (missing_count_expr == 0), lit("warning"))
    .otherwise(lit(None))
)

# Filter only the records with 'error' or 'warning' status
df_errors_warnings = df.filter(col("status").isNotNull())

df_errors_warnings.write.format("delta").mode("overwrite").save(TARGET_LOCATION)
