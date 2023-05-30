import json


class TableConfigValidator:
    def __init__(self, json_file_name):
        self.json_data = {}
        self.cluster_name = json_file_name
        with open(f"data/tables/{json_file_name}.json") as json_data_file:
            self.json_data = json.load(json_data_file)

    def validate(self):
        required_keys = [self.cluster_name]

        # Check if all required keys are present
        for key in required_keys:
            if key not in self.json_data:
                return False

        # Check specific validations for each key
        if not self._validate_table_configs(self.json_data["sample_tables"]):
            return False

        return True

    def _validate_table_configs(self, sample_tables):
        if not isinstance(sample_tables, dict):
            return False

        for table_name, table_data in sample_tables.items():
            if not self._validate_table_data(table_data):
                return False

        return True

    def _validate_table_data(self, table_data):

        # required keys for all the tables
        required_keys = [
            "schema",
            "columns",
            "pk",
            "etl_type"
        ]

        # Check if all required keys are present
        for key in required_keys:
            if key not in table_data:
                return False

        if not isinstance(table_data["schema"], str):
            return False

        if not isinstance(table_data["columns"], str):
            return False

        if not isinstance(table_data["pk"], str):
            return False

        if not isinstance(table_data["etl_type"], str):
            return False

        # validate for each etl types
        etl_type = table_data["etl_type"]

        if etl_type == "single-ts-based-cdc":
            if "cdc_ts_column" not in table_data:
                return False
            if not isinstance(table_data["cdc_ts_column"], str):
                return False

        elif etl_type == "business-ts-based-cdc":
            if "inserted_ts_column" not in table_data or "updated_ts_column" not in table_data or "retain_days" not in table_data:
                return False
            if not isinstance(table_data["inserted_ts_column"], str) or not isinstance(table_data["updated_ts_column"],
                                                                                       str) or not isinstance(
                    table_data["retain_days"], int):
                return False

        elif etl_type not in ["full-load", "business-ts-based-cdc", "single-ts-based-cdc", "query-load"]:
            return False

        return True
