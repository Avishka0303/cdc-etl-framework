import argparse

from data.others.sites import OPCO_IDS
from etl_core.etl_core import ETLCore
from etl_core.loader import Loader
from utils.db_connector import get_sod_connection, get_cache_db_connection
from utils.etl_logger import get_logger
from utils.load_config import get_table_definition


class FullLoader(ETLCore):
    """
    this class responsible for manual full load a table
    """

    def __init__(self, table_name, opco_list, log, source_connection, target_connection):
        super().__init__(opco_list, log, source_connection, target_connection)
        self.table_name = table_name
        self.table_definition = get_table_definition(table_name)
        self.exc = None

    def full_load_table(self):
        try:
            full_load_by_time_window(table_name=self.table_name,
                                          columns=self.table_definition['columns'],
                                          cdc_column=self.table_definition['cdc_key'],
                                          retain_duration_days=self.table_definition['retain_days'])
        except BaseException as be:
            self.exc = be


def main(table_name, truncate_option):
    try:
        sod_connection = get_sod_connection()
        cache_db_connection = get_cache_db_connection()
        loader = Loader(cache_db_connection)
        opco_ids_to_insert = OPCO_IDS

        if truncate_option == 'Y':
            # secondary acceptance:
            acceptance = input(f"Please confirm the truncation of the table : {table_name} (Y/N) : \n")
            if acceptance == 'Y':
                loader.delete_all_records(table_name)
                logger.warning(f'--- table:{table_name} was deleted successfully ---')
            else:
                logger.warning(f'---Truncation of table {table_name} cancelled---')

        else:
            logger.warning(f"--- table:{table_name} will not be truncated. ---")
            last_inserted_opco = loader.delete_last_inserted_opco(table_name=table_name,
                                                                  opco_id_identifier='sv_opco_nbr')
            logger.info('---last inserted opco records deleted---')
            opco_ids_to_insert = [opco for opco in OPCO_IDS if opco >= last_inserted_opco]

        sod_etl = FullLoader(table_name=table_name,
                             opco_list=opco_ids_to_insert,
                             log=logger,
                             source_connection=sod_connection,
                             target_connection=cache_db_connection)

        logger.info(f'--- Full loader started for table:{table_name} ---')
        sod_etl.full_load_table()

    except Exception as ex:
        logger.error('--- Error occurred while running the program ::: %s ---', ex, exc_info=True)
    else:
        logger.info(f"--- full load for {table_name} finished successfully")


if __name__ == '__main__':
    """
    for execute the full load on a table for given retain days in config file use following command in project root. 
    python3 -m etl_src.sample_etls.etl_optimizer.full_loader <table_name>
    """
    logger = None
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("-trn", type=str, help="Truncate the table 'Y'- truncate 'N'- don't truncate")
        parser.add_argument("-tbl", type=str, help="The SOD table name")

        args = parser.parse_args()
        tbl_name = args.tbl
        del_opt = args.trn

        # logger object for logs
        logger = get_logger('full_loader')

        # set default delete option as NO
        if del_opt is None:
            del_opt = 'N'

        main(table_name=tbl_name, truncate_option=del_opt)

    except Exception as e:
        logger.error('--- Error occurred while running the full load script:%s ---', e, exc_info=True)
