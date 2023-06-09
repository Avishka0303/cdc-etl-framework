import threading

from etl_core.etl_core import ETLCore
from utils.load_config import get_table_configurations


class TransactionETL(ETLCore):
    TABLE_NAME = 'transaction'

    def __init__(self, logger):
        super().__init__(logger)
        self.table_definition = get_table_configurations(self.TABLE_NAME)
        self.exc = None

    def __str__(self):
        return "transaction etl"

    # @override
    def run(self):
        try:
            self.single_timestamp_based_cdc_etl(schema=self.table_definition["schema"],
                                                table_name=self.TABLE_NAME,
                                                columns=self.table_definition["columns"],
                                                pk_columns=self.table_definition["pk"],
                                                cdc_column=self.table_definition["cdc_ts_column"])
        except BaseException as e:
            self.exc = e
