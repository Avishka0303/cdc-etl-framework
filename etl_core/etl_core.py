import time
from datetime import datetime, timedelta
from threading import Thread

from etl_core.extractor import Extractor
from etl_core.loader import Loader
from etl_core.purger import full_purge
from utils.list_manipulator import get_column_index_from_csv


class ETLCore(Thread):

    def __init__(self, logger):
        Thread.__init__(self)
        self.logger = logger
        self.exc = None

    def join(self, **kwargs):
        Thread.join(self)
        if self.exc:
            raise self.exc

    def full_load_table(self, schema, table_name, columns, condition=None, mat_view=None):
        """
        copy and load a table from one source to another.
        :param schema:
        :param mat_view: view for update
        :param table_name: table name
        :param columns: for get all the columns pass '*' , otherwise pass comma seperated column list
        :param condition: condition for table , Default is None.
        """

        # full purge the table
        full_purge(table_name=table_name)

        # dynamically generate union query
        query_string = f"SELECT {columns} " \
                       f"FROM {schema}.{table_name} "
        if condition is not None:
            query_string += f'where {condition}'

        extractor = Extractor()
        loader = Loader()

        # fetch records from tables and insert into cache db
        for records in extractor.fetch_records_by_query(query_string):
            if len(records) > 0:
                print(f'--- record count of {table_name} : {len(records)} ---')
                loader.insert_records(schema=schema,
                                      table_name=table_name,
                                      columns=columns,
                                      records=records)

        # refresh materialize view if exists.
        if mat_view is not None:
            loader.refresh_view(schema=schema, view_name=mat_view)
            mat_st_time = time.time()
            self.logger.info(
                f"--- Materialized view updated {mat_view} elapsed time: {time.time() - mat_st_time}s ---\n",
                extra={
                    'materialized view': f'{mat_view}',
                    'elapsed_time': f'{time.time() - mat_st_time}'
                })

    def full_load_by_time_window(self, schema, table_name, columns, cdc_column, retain_duration_days):
        """
        Retrieve data by time window. Can use for heavy full loads.
        :param schema:
        :param retain_duration_days: how many past days we should look for data
        :param table_name: table name both in tables and cache_db
        :param columns: columns of interest
        :param cdc_column: change data capture column
        """

        extractor = Extractor()
        loader = Loader()

        retain_duration = retain_duration_days * 24  # total data retaining hours
        hours_per_time_window = 1  # size of the time window

        # oldest date time value to retain data.
        lower_limit_ts = datetime.now() - timedelta(hours=retain_duration)
        # first time window cap
        upper_limit_ts = lower_limit_ts + timedelta(hours=hours_per_time_window)

        loaded_hours, batch_count, record_count = 0, 1, 0

        while loaded_hours <= retain_duration:
            self.logger(f'--- loading data from ::: {lower_limit_ts} to ::: {upper_limit_ts} ---')
            for records in extractor.fetch_records_between(schema=schema,
                                                           table_name=table_name,
                                                           columns=columns,
                                                           cdc_column=cdc_column,
                                                           lower_cap=lower_limit_ts,
                                                           upper_cap=upper_limit_ts):  # get records between

                # target db update only for actual records.
                if len(records) > 0:
                    loader.insert_records(table_name=table_name, columns=columns, records=records)
                    batch_count += 1
                    record_count += len(records)
                    print(f'--- batch: {batch_count} successfully. total inserted records ::: {record_count} ---')

            loaded_hours += 1
            lower_limit_ts = lower_limit_ts + timedelta(hours=hours_per_time_window)
            upper_limit_ts = lower_limit_ts + timedelta(hours=hours_per_time_window)

        extractor.close_connection()
        loader.close_connection()

    def single_timestamp_based_cdc_etl(self, schema, table_name, columns, pk_columns, cdc_column, cdc_column_has_milliseconds=True):
        """
        insert data from source to target using single column based change data capturing
        :param schema:
        :param cdc_column_has_milliseconds:
        :param pk_columns: primary key columns
        :param table_name: source and target table name
        :param columns: columns of interest
        :param cdc_column: auditing column for change capturing by time
        """

        loader = Loader()
        extractor = Extractor()

        search_time = time.time()
        # get limits records [[001,max_date],[002,max_date],...]
        limit_records = loader.get_cdc_limits(schema=schema,
                                              table_name=table_name,
                                              cdc_column=cdc_column)

        search_time_elapsed = time.time() - search_time

        self.logger.info(f'--- Elapsed time for get last updated times: {search_time_elapsed}s ---',
                         extra={
                             'etl_event': 'CDC-Limit-Fetcher',
                             'elapsed_time': f'{search_time_elapsed}'
                         })

        batch_count, record_count = 0, 0

        last_recorded_timestamp, st_time = limit_records[0][0].strftime('%Y-%m-%d %H:%M:%S.%f'), time.time()

        self.logger.info(f"--- start record fetching for table ::: {table_name} "
                         f"--- last updated timestamp ::: {last_recorded_timestamp} --- ")

        primary_key_indexes = get_column_index_from_csv(column_csv=columns,
                                                        idx_columns=pk_columns)

        # iterating over batches
        for batch in extractor.fetch_records_by_cdc(schema=schema,
                                                    table_name=table_name,
                                                    columns=columns,
                                                    pk_columns=pk_columns,
                                                    cdc_column=cdc_column,
                                                    cdc_limit="'" + last_recorded_timestamp + "'",
                                                    cdc_key_with_time=cdc_column_has_milliseconds):
            batch_size = len(batch)

            if batch_size > 0:
                # delete existing columns.
                pk_values_sliced = [[row[i] for i in primary_key_indexes] for row in batch]

                deleted_row_count = loader.delete_existing_records(schema=schema,
                                                                   table_name=table_name,
                                                                   pk_column_csv=pk_columns,
                                                                   composite_key_list=pk_values_sliced)

                print(f'--- batch: {batch_count} insert started. deleted existing row count: {deleted_row_count} -')

                # insert batch of records
                loader.insert_records(schema=schema,
                                      table_name=table_name,
                                      columns=columns,
                                      records=batch)

                print(f'batch: {batch_count} successfully. total inserted records : {record_count + batch_size}')

                record_count, batch_count = record_count + batch_size, batch_count + 1

        self.logger.info(f" --- Insertion completed for table: {table_name} => record count: {record_count} "
                         f"elapsed time: {time.time() - st_time} s ---\n",
                         extra={
                             'table_name': f'{table_name}',
                             'elapsed_time': f'{time.time() - st_time}'
                         })

        extractor.close_connection()
        loader.close_connection()

    def business_columns_based_cdc_etl(self, schema, table_name, columns, pk_columns, add_date_column, upd_date_column):
        """
        insert data from source to target using business columns based change data capturing
        :param schema: source and target schema.
        :param upd_date_column: update tracking column
        :param add_date_column: insertion tracking column
        :param pk_columns: primary key columns
        :param table_name: source and target table name
        :param columns: columns of interest
        """

        loader = Loader()
        extractor = Extractor()

        search_start_time = time.time()

        # get add_time of lastly inserted records [[001,max_date(added-date)],[002,max_date(added_date)],...]
        latest_added_ts = loader.get_last_added_dates(schema=schema,
                                                      table_name=table_name,
                                                      add_column=add_date_column,
                                                      upd_column=upd_date_column)

        # get add_time of lastly updated records [[001,max_date(updated_date)],[002,max_date(added_date)],...]
        latest_updated_ts = loader.get_last_updated_dates(schema=schema,
                                                          table_name=table_name,
                                                          add_column=add_date_column,
                                                          upd_column=upd_date_column)

        search_time_elapsed = time.time() - search_start_time

        self.logger.info(f'--- capture lastly inserted ts --> Elapsed Time: {search_time_elapsed} s ---')

        # get indexes of primary keys.
        primary_key_indexes = get_column_index_from_csv(column_csv=columns,
                                                        idx_columns=pk_columns)

        # --------------------------------------------------------------------------------------------------------------
        # insert newly added records using added time column
        batch_count, record_count = 0, 0

        limit, st_time = latest_added_ts[0][0].strftime('%Y-%m-%d %H:%M:%S.%f'), time.time()

        self.logger.info(f"--- start to fetch lastly added record for table ::: {table_name} "
                         f"--- ts ::: {add_date_column} > {limit} ---")

        for batch in extractor.fetch_records_by_cdc(schema=schema,
                                                    table_name=table_name,
                                                    columns=columns,
                                                    pk_columns=pk_columns,
                                                    cdc_column=add_date_column,
                                                    cdc_limit="'" + limit + "'"):
            batch_size = len(batch)

            if batch_size > 0:
                # delete existing columns.
                pk_values_sliced = [[row[i] for i in primary_key_indexes] for row in batch]

                deleted_row_count = loader.delete_existing_records(schema=schema,
                                                                   table_name=table_name,
                                                                   pk_column_csv=pk_columns,
                                                                   composite_key_list=pk_values_sliced)

                print(f'--- batch: {batch_count} insert started. deleted existing row count:{deleted_row_count}---')

                # insert batch of records
                loader.insert_records(schema=schema,
                                      table_name=table_name,
                                      columns=columns,
                                      records=batch)

                print(f'--- batch ::: {batch_count} inserted successfully. '
                      f'--- total inserted records ::: {record_count + batch_size}')

                record_count, batch_count = record_count + batch_size, batch_count + 1

        self.logger.info(f"--- # added # records inserted for table ::: {table_name} "
                         f"--- record count::: {record_count} "
                         f"--- elp.time: {time.time() - st_time} s ---")

        # --------------------------------------------------------------------------------------------------------------

        # insert updated records using upd_date time column
        for record in latest_updated_ts:
            batch_count, record_count = 0, 0

            # ignore records dont have upd date
            if record[1] is None:
                continue

            limit, st_time = latest_updated_ts[0][0].strftime('%Y-%m-%d %H:%M:%S.%f'), time.time()

            self.logger.info(f"--- start to fetch last updated records for table ::: {table_name} "
                             f"--- {upd_date_column} ::: {limit} ---")

            for batch in extractor.fetch_records_by_cdc(schema=schema,
                                                        table_name=table_name,
                                                        columns=columns,
                                                        pk_columns=pk_columns,
                                                        cdc_column=upd_date_column,
                                                        cdc_limit="'" + limit + "'"):
                batch_size = len(batch)

                if batch_size > 0:
                    # delete existing columns.
                    pk_values_sliced = [[row[i] for i in primary_key_indexes] for row in batch]

                    deleted_row_count = loader.delete_existing_records(schema=schema,
                                                                       table_name=table_name,
                                                                       pk_column_csv=pk_columns,
                                                                       composite_key_list=pk_values_sliced)

                    print(f'--- batch: {batch_count} insert started. deleted existing row count: {deleted_row_count} -')

                    # insert batch of records
                    loader.insert_records(schema=schema,
                                          table_name=table_name,
                                          columns=columns,
                                          records=batch)

                    print(f'--- batch: {batch_count} inserted successfully. '
                          f'--- total inserted records : {record_count + batch_size} ')

                    record_count, batch_count = record_count + batch_size, batch_count + 1

            self.logger.info(f"--- Newly # updated # records inserted for for table ::: {table_name} "
                             f"--- record count ::: {record_count} "
                             f"--- elp.time: {time.time() - st_time} s ---\n")

        extractor.close_connection()
        loader.close_connection()

    def run_query_customized_etl(self, query, target_schema, target_table, target_columns, mat_view):
        """
        dynamically create a union query for given table name and condition
        :param mat_view:
        :param target_columns:
        :param query:
        :param target_table:
        :param target_schema:
        """

        extractor = Extractor()
        loader = Loader()

        # fetch records from tables and insert into cache db
        for records in extractor.fetch_records_by_query(query=query):
            if len(records) > 0:
                loader.insert_records(schema=target_schema,
                                      table_name=target_table,
                                      columns=target_columns,
                                      records=records)

        # refresh materialize view if exists.
        if mat_view is not None:
            loader.refresh_view(schema=target_schema, view_name=mat_view)
            mat_st_time = time.time()
            self.logger.info(
                f"--- Materialized view updated {mat_view} elapsed time: {time.time() - mat_st_time}s ---\n",
                extra={
                    'materialized view': f'{mat_view}',
                    'elapsed_time': f'{time.time() - mat_st_time}'
                })


