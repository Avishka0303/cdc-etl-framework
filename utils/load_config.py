# this is for reading configs files for prod and dev environment
import json
import os


def get_environment():
    """
    check program running environment
    :return: "dev" for development environment "prod" for production environment
    """
    try:
        etl_environment = os.environ['ETL_ENV']
    except Exception as e:
        etl_environment = 'dev'
        print('Error: Environment variable {} not set. run in dev mode'.format(e))
    return etl_environment


def get_db_credentials():
    """
    read .json configuration file related to environment
    :return: configuration dictionary
    """
    env = get_environment()

    with open(f"config/{env}_config.json") as json_data_file:
        config_data = json.load(json_data_file)
    return config_data


def get_main_configurations():
    with open(f"config/main_config.json") as json_data_file:
        config_data = json.load(json_data_file)
    return config_data


def get_table_configurations(table_name):
    """
    retrieve table definitions
    :param table_name: table name
    :return: dictionary of table definition
    """
    with open(f"data/tables/sample_tables.json") as json_data_file:
        config_data = json.load(json_data_file)
    return config_data["sample_tables"][table_name]
