import psycopg2
import mysql

from utils.load_config import get_config_dictionary


def get_connection(db_name):
    """
    Connection Factory to create database connections
    :param db_name: database name as indicated in config files
    :return: connection object
    """
    db_credentials = get_config_dictionary()['db_credentials'][db_name]

    db_type = db_credentials["db_type"]

    if db_type == 'PSQL':

        keepalive_kwargs = {
            "keepalives": 1,
            "keepalives_idle": 360,
            "keepalives_interval": 5,
            "keepalives_count": 5,
        }

        return psycopg2.connect(
            host=db_credentials['host'],
            port=db_credentials['port'],
            database=db_credentials['db_name'],
            user=db_credentials['username'],
            password=db_credentials['password'],
            **keepalive_kwargs)

    elif db_type == 'MYSQL':

        return mysql.connector.connect(
            host=db_credentials["host"],
            user=db_credentials["user"],
            password=db_credentials["password"])
