import threading

from etl_core.etl_core import ETLCore
from utils.load_config import get_table_definition


class BatchETL(ETLCore):
    TABLE_NAME = 'batch'

    def __init__(self,logger):
        super().__init__(logger)
        self.table_definition = get_table_definition(self.TABLE_NAME)
        self.exc = None

    def __str__(self):
        return "batch etl"

    def join(self, **kwargs):
        threading.Thread.join(self)
        if self.exc:
            raise self.exc

    # @override
    def run(self):
        try:
            self.cdc_using_business_columns(schema=self.table_definition["schema"],
                                            table_name=self.TABLE_NAME,
                                            columns=self.table_definition["columns"],
                                            pk_columns=self.table_definition["pk"],
                                            add_date_column=self.table_definition["inserted_ts_column"],
                                            upd_date_column=self.table_definition["updated_ts_column"])
        except BaseException as e:
            self.exc = e
