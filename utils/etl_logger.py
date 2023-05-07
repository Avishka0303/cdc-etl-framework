import logging
import logging.config
import os
import sys

from logging.handlers import TimedRotatingFileHandler
from pythonjsonlogger import jsonlogger
from utils.load_config import get_etl_configurations


def get_logger(name):
    """
    write json logs to /var/log/odi-cache-db-etl-logs folder
    :param name: etl name
    :return: logger object
    """
    etl_configurations = get_etl_configurations()
    log_base_path = etl_configurations["LOG_PATH"]
    log_file_prefix = etl_configurations["LOG_PREFIX"]

    # .log file path for the application filename:<etl_name>.log
    log_path = f'{log_base_path}/{log_file_prefix}_{name}.log'

    # create the log directory if it does not exist
    os.makedirs(
        name=os.path.dirname(log_path),
        exist_ok=True,
        mode=0o0777)  # this subtracts with umask

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # json formatter
    formatter = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(name)s '
                                         '%(thread)d [%(filename)s:%(lineno)d] %(message)s')

    # for rotating logger files and keep backup for 7 days.
    handler = TimedRotatingFileHandler(log_path,
                                       when="d",
                                       interval=1,
                                       backupCount=7)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # streaming logs to console.
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

    return logger
