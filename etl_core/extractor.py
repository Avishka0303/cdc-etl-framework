from utils.db_connector import get_connection
from utils.load_config import get_main_configurations


class Extractor:
    """
    This class responsible for extraction of data from source
    """

    def __init__(self):
        self.source_connection = get_connection(db_name="source")
        self.BATCH_SIZE = get_main_configurations()["EXTRACTOR_BATCH_SIZE"]

    def fetch_records_by_cdc(self, schema, table_name, columns, pk_columns, cdc_column, cdc_limit,
                             cdc_key_with_time=True):
        """
        select records by change data capturing.
        """
        comparator = '>'
        if not cdc_key_with_time:
            comparator = '>='

        query = f"select {columns} " \
                f"from {schema}.{table_name} " \
                f"where {cdc_column} {comparator} {cdc_limit} order by {pk_columns} "

        fetch_cursor = self.source_connection.cursor()
        fetch_cursor.execute(query)
        row_count = fetch_cursor.rowcount
        print(f'--- Total row count: {row_count} ---')
        tuples = fetch_cursor.fetchmany(self.BATCH_SIZE)

        yield tuples
        while tuples:
            tuples = fetch_cursor.fetchmany(self.BATCH_SIZE)
            yield tuples

        fetch_cursor.close()

    def fetch_records_by_query(self, query):
        """
        Select records for supplied query
        """
        fetch_cursor = self.source_connection.cursor()
        fetch_cursor.execute(query)
        row_count = fetch_cursor.rowcount
        print(f'--- Total row count: {row_count} ---')
        tuples = fetch_cursor.fetchmany(self.BATCH_SIZE)
        yield tuples
        while tuples:
            tuples = fetch_cursor.fetchmany(self.BATCH_SIZE)
            yield tuples

        fetch_cursor.close()

    def fetch_records_between(self, schema, table_name, columns, cdc_column, lower_cap, upper_cap):
        """
        Select and return the rows that are newer than the capture limit.
        This method yields records by the batch size
        """
        fetch_query = f"select {columns} " \
                      f"from {schema}.{table_name} " \
                      f"where {cdc_column} >= '{lower_cap}' " \
                      f"    and {cdc_column} < '{upper_cap}'"

        fetch_cursor = self.source_connection.cursor()
        fetch_cursor.execute(fetch_query)
        row_count = fetch_cursor.rowcount
        print(f'--- Total row count: {row_count} ---')
        records = fetch_cursor.fetchmany(self.BATCH_SIZE)
        yield records
        while records:
            records = fetch_cursor.fetchmany(self.BATCH_SIZE)
            yield records

        fetch_cursor.close()

    def close_connection(self):
        self.source_connection.close()
