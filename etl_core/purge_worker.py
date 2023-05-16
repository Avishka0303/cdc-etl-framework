import argparse
import time

from etl_core.loader import Loader
from utils.load_config import get_table_definition
from utils.etl_logger import get_logger


def full_purge(table_name):
    tbl_def = get_table_definition(table_name)
    batch_loader = Loader()
    batch_loader.delete_all_records(schema=tbl_def["schema"], table_name=table_name)
    batch_loader.close_connection()


def time_based_purge(table_name):
    tbl_def = get_table_definition(table_name)
    batch_loader = Loader()
    batch_loader.delete_records_by_time(table_name=table_name,
                                        time_column=tbl_def["cdc_ts_column"],
                                        time_in_days=tbl_def["retain_days"])
    batch_loader.close_connection()


def main(table_name, purge_option):
    tbl_def = get_table_definition(table_name)
    logger.info(f'--- purge_{table_name}_etl started. ---')
    if tbl_def is not None:
        if purge_option == 'F':
            full_purge(table_name)
        elif purge_option == 'P':
            st_time = time.time()
            time_based_purge(table_name)
            elapsed_time = time.time() - st_time
            logger.info(msg=f'Elapsed time for Purge ETL {table_name} : {elapsed_time}s',
                        extra={'etl_name': f'purge_{table_name}_etl', 'elapsed_time': elapsed_time})


if __name__ == '__main__':
    logger = None
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("-popt", type=str, help="The purge option 'F'- Full purge 'P'- Partial purge(Time based)")
        parser.add_argument("-tbl", type=str, help="The table name")

        args = parser.parse_args()
        tbl_name = args.tbl
        purge_opt = args.popt

        logger = get_logger(f'purge_{tbl_name}_etl')
        main(table_name=tbl_name, purge_option=purge_opt)
    except Exception as e:
        logger.error(f'purge failed. %s', e, exc_info=True)
