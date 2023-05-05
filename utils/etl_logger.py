import logging
import logging.config
import os
import sys

from logging.handlers import TimedRotatingFileHandler
from pythonjsonlogger import jsonlogger


def get_logger(name):
    """
    write json logs to /var/log/odi-cache-db-etl-logs folder
    :param name: etl name
    :return: logger object
    """
    # .log file path for the application filename:ocde_<etl_name>.log
    log_path = '/var/log/odi-cache-db-etl-logs/ocde_{}.log'.format(name)

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
