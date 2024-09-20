"""Bronze layer loader."""
import json
import logging
import sys
from datetime import date

import requests

sys.path.append("/opt/airflow/breweries_use_case/utils")
import render

logging.basicConfig(level=logging.INFO)

# Setup variables
ENDPOINT = render.get_yaml_value("breweries_endpoint")
BRONZE_LOCATION = render.get_yaml_value("bronze_location")
FILE_NAME = render.get_yaml_value("file_name")

# Get response
response = requests.get(ENDPOINT).json()

# Serialize json
json_object = json.dumps(response, indent=4)

# Define file name
file_name = BRONZE_LOCATION + FILE_NAME + date.today().strftime("%Y%m%d") + ".json"

# Write data
try:
    with open(file_name, "w") as outfile:
        outfile.write(json_object)
    logging.info("Process finished successfully!")
except Exception as e:
    logging.error(e)
