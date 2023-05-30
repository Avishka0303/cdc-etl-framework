import threading

from etl_core.etl_core import ETLCore
from utils.load_config import get_table_configurations


class UserTransactionETL(ETLCore):
    TABLE_NAME = 'user_transaction_aggr'

    def __init__(self, logger):
        super().__init__(logger)
        self.table_definition = get_table_configurations(self.TABLE_NAME)
        self.exc = None

    def __str__(self):
        return "user transaction aggregator etl"

    def run(self):
        try:
            with open('data/queries/sample_aggregation.sql', 'r') as f:
                query = f.read()
                self.run_query_customized_etl(query=query,
                                              target_schema=self.table_definition["schema"],
                                              target_table=self.TABLE_NAME,
                                              target_columns=self.table_definition["columns"],
                                              mat_view=self.table_definition["mat_view"])
        except BaseException as e:
            self.exc = e
