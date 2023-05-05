from utils.db_connector import get_connection


class Loader:
    """
    This class responsible for Load the extracted and transformed data into destination
    """

    def __init__(self):
        self.target_connection = get_connection(db_name="target")

    def get_cdc_limits(self, schema, table_name, cdc_column):
        """
        Get max cdc column value for each opco
        :param schema:
        :param table_name:
        :param cdc_column: date time column for track down changes
        :return: list : [['001','DATE'],['002','DATE']]
        """

        fetch_cursor = self.target_connection.cursor()
        query = f"select max({cdc_column}) " \
                f"from {schema}.{table_name} " \
                # f"where {cdc_column} < now() + interval '1 days' "
        fetch_cursor.execute(query)
        tuples = fetch_cursor.fetchall()

        fetch_cursor.close()

        return tuples

    def get_last_added_dates(self, schema, table_name, add_column, upd_column):
        """
        Get last add_date in cache db (used in business column cdc methods)
        :param schema:
        :param add_column: added data tracking column (date time)
        :param upd_column: updated tracking column (date time)
        :param table_name: table name
        :return: list : [['001','DATE'],['002','DATE']]
        """
        fetch_cursor = self.target_connection.cursor()
        query = f"select max({add_column}) " \
                f"from {schema}.{table_name} "

        fetch_cursor.execute(query)
        tuples = fetch_cursor.fetchall()

        fetch_cursor.close()

        return tuples

    def get_last_updated_dates(self, schema, table_name, add_column, upd_column):
        """
        Get last upd_date in cache db (used in business column cdc methods)
        :param add_column: added data tracking column (date time)
        :param upd_column: updated tracking column (date time)
        :param table_name: table name
        :param opco_id_column: column for identify opco id in a record
        :param opco_id_csv: list of opco ids as a csv
        :return: list : [['001','DATE'],['002','DATE']]
        """
        fetch_cursor = self.target_connection.cursor()
        query = f"select max({upd_column}) " \
                f"from {schema}.{table_name} "

        fetch_cursor.execute(query)
        tuples = fetch_cursor.fetchall()

        fetch_cursor.close()

        return tuples

    def get_cdc_limits_from_cdc_logs(self, table_name, opco_id_csv):
        """
        Get max cdc column value for each opco
        :param table_name:
        :param opco_id_csv: list of opco ids as a csv
        :return: list : [['001','DATE'],['002','DATE']]
        """

        fetch_cursor = self.target_connection.cursor()
        query = f"select * " \
                f"from {self.schema}.cdc_logs " \
                f"where table_name={table_name} and opco_id in ('{opco_id_csv}')"
        fetch_cursor.execute(query)
        tuples = fetch_cursor.fetchall()

        fetch_cursor.close()

        return tuples

    def delete_all_records(self, table_name):
        """
        Delete all the records from the table
        """
        delete_query = f"delete from {self.schema}.{table_name}"
        target_cursor = self.target_connection.cursor()
        target_cursor.execute(delete_query)
        self.target_connection.commit()
        target_cursor.close()

    def delete_existing_records(self, schema, table_name, pk_column_csv, composite_key_list):
        """
        Delete the updated rows that already exist in the target database using primary key
        :param composite_key_list: list of tuples of primary key value.
        :param pk_column_csv: composite keys
                            composite_key_lists = [
                                    ('key1_value1', 'key2_value1'),
                                    ('key1_value2', 'key2_value2'),
                                    ('key1_value3', 'key2_value3')
                            ]
        :param table_name:
        """
        batch_size = 5000
        rows_deleted = 0
        for batch_start in range(0, len(composite_key_list), batch_size):
            batch = composite_key_list[batch_start:batch_start + batch_size]
            pk_values = tuple([tuple(row) for row in batch])
            delete_query = f"DELETE FROM {schema}.{table_name} WHERE ({pk_column_csv}) IN %s "
            target_cursor = self.target_connection.cursor()
            delete_query = target_cursor.mogrify(delete_query, (pk_values,))
            target_cursor.execute(delete_query)
            rows_deleted += target_cursor.rowcount

        return rows_deleted

    def delete_records_by_time(self, table_name, time_column, time_in_days, latest=False):
        """
        Delete records by time if time_column < time_in_days
        :param latest: true if we want to delete latest record
        :param time_in_days: time range
        :param time_column:
        :param table_name:
        """
        comparator = '<'
        if latest:
            comparator = '>='

        delete_query = f"delete " \
                       f"from {self.schema}.{table_name} " \
                       f"where {time_column} {comparator} current_date - interval '{time_in_days} Day'"
        target_cursor = self.target_connection.cursor()
        target_cursor.execute(delete_query)
        self.target_connection.commit()

        target_cursor.close()

    def insert_records(self, schema, table_name, columns, records):
        """
        Insert the records to target database batch-wise
        """
        target_cursor = self.target_connection.cursor()
        parameters_string = '(' + (', '.join(['%s'] * len(records[0]))) + ')'  # build parameter string
        args = ','.join(target_cursor.mogrify(parameters_string, i).decode('utf-8') for i in records)
        insert_query = f'insert into {schema}.{table_name} ({columns}) values '
        target_cursor.execute(insert_query + args)
        self.target_connection.commit()
        target_cursor.close()

    def refresh_view(self, schema, view_name):
        """
        Refresh materialized views for truncate load tables
        """
        target_cursor = self.target_connection.cursor()
        view_update_query = f"REFRESH MATERIALIZED VIEW {schema}.{view_name}"
        target_cursor.execute(view_update_query)
        self.target_connection.commit()
        target_cursor.close()

    def vacuum_table(self, table_name):
        """
        Vacuum tables
        """
        target_cursor = self.target_connection.cursor()
        vacuum_query = f"VACUUM (ANALYZE) {table_name}"
        target_cursor.execute(vacuum_query)
        self.target_connection.commit()
        target_cursor.close()

    def delete_last_inserted_opco(self, table_name, opco_id_identifier):
        """
        Get delete records for last opco
        :param table_name:
        :param opco_id_identifier: column for identify opco id in a record
        :return: list : [['001','DATE'],['002','DATE']]
        """

        fetch_cursor = self.target_connection.cursor()
        query = f"select max({opco_id_identifier}) from {self.schema}.{table_name}"
        fetch_cursor.execute(query)
        tuples = fetch_cursor.fetchall()
        max_opco = tuples[0][0]

        print(max_opco, 'going to delete')

        query = f"delete from {self.schema}.{table_name} where {opco_id_identifier}='{max_opco}'"
        fetch_cursor.execute(query)
        self.target_connection.commit()

        fetch_cursor.close()

        return max_opco

    def close_connection(self):
        self.target_connection.close()
