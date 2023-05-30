import threading

from etl_core.etl_core import ETLCore
from utils.load_config import get_table_configurations


class UserETL(ETLCore):
    TABLE_NAME = 'user'

    def __init__(self, logger):
        super().__init__(logger)
        self.table_definition = get_table_configurations(self.TABLE_NAME)
        self.exc = None

    def __str__(self):
        return "user etl"

    def run(self):
        try:
            self.full_load_table(schema=self.table_definition["schema"],
                                 table_name=self.TABLE_NAME,
                                 columns=self.table_definition["columns"],
                                 condition=self.table_definition["condition"],
                                 mat_view=self.table_definition["mat_view"])
        except BaseException as e:
            self.exc = e
