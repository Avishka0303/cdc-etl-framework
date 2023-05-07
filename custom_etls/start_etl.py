import importlib
import sys
import time

from utils.custom_exceptions import CustomException
from utils.etl_logger import get_logger
from utils.load_config import get_etl_dictionary


def main(etl_name):

    # load ETL configurations.
    etl_dictionary = get_etl_dictionary()
    etl_src_module = etl_dictionary["ETL_SRC_MODULE"]
    etl_class_name = etl_dictionary["ETL_REGISTER"][etl_name]

    # get table name using etl_name
    table_name, _, suffix = etl_name.rpartition('_')

    # import the module dynamically
    module = importlib.import_module(f"{etl_src_module}.{etl_name}.{etl_name}")

    # Get the class you want to import
    etl_class = getattr(module, etl_class_name)

    threads = []

    # Now you can use the imported class
    etl_instance = etl_class(logger=logger)
    etl_instance.daemon = True
    threads.append(etl_instance)

    # start measuring time for the etl
    start_time = time.time()
    etl_instance.start()
    try:
        for thread in threads:
            thread.join()
    except Exception as ex:
        logger.error('Error occurred while running the ETL %s', ex, exc_info=True)
        exit(-1)

    elapsed_time = time.time() - start_time

    logger.info(f'--- ETL: {etl_name} Finished ! Elapsed Time: {elapsed_time}---',
                extra={
                    'etl_name': f'{etl_name}',
                    'elapsed_time': f'{elapsed_time}'
                })


if __name__ == '__main__':
    logger = None
    try:
        etl_name_to_verify = sys.argv[1]
        # validate the command line argument.
        if etl_name_to_verify in get_etl_dictionary()["ETL_REGISTER"]:
            logger = get_logger(etl_name_to_verify)
            main(sys.argv[1])
        else:
            logger = get_logger("unknowns")
            raise CustomException("Unknown ETL name. Check the registry")
    except Exception as e:
        logger.error('Unknown ETL name: %s', e, exc_info=True)
