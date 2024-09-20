"""Render."""

import os

import yaml


def get_yaml_value(param: str) -> any:
    """Get the parameter from the yaml file.

    :param param: parameter to be rendered.
    :return: the value of a given parameter from the yaml file.
    """
    yaml_path = "/opt/airflow/breweries_use_case/configs/params.yaml"
    yaml_path = os.path.join(os.path.dirname(__file__), yaml_path)
    try:
        with open(f"{yaml_path}", "r") as f:
            config = yaml.safe_load(f)
    except Exception as e:
        raise e
    return config[param]
