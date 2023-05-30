from utils.load_config import get_main_configurations


class MainConfigValidator:
    def __init__(self):
        self.json_data = get_main_configurations()

    def validate(self):
        required_keys = [
            "ETL_REGISTER",
            "ETL_SRC_MODULE",
            "LOG_PATH",
            "LOG_PREFIX",
            "TABLE_CONFIG_JSON",
            "EXTRACTOR_BATCH_SIZE"
        ]

        # Check if all required keys are present
        for key in required_keys:
            if key not in self.json_data:
                return False

        # Check specific validations for each key
        if not self._validate_etl_register(self.json_data["ETL_REGISTER"]):
            return False

        if not self._validate_etl_src_module(self.json_data["ETL_SRC_MODULE"]):
            return False

        if not self._validate_log_path(self.json_data["LOG_PATH"]):
            return False

        if not self._validate_log_prefix(self.json_data["LOG_PREFIX"]):
            return False

        if not self._validate_table_json_path(self.json_data["TABLE_CONFIG_JSON"]):
            return False

        if not self._validate_extractor_batch_size(self.json_data["EXTRACTOR_BATCH_SIZE"]):
            return False

        return True

    def _validate_etl_register(self, etl_register):
        if not isinstance(etl_register, dict):
            return False

        required_etl_keys = [
            "user_etl",
            "transaction_etl",
            "batch_etl",
            "user_transaction_etl"
        ]

        for key in required_etl_keys:
            if key not in etl_register or not isinstance(etl_register[key], str):
                return False

        return True

    def _validate_etl_src_module(self, etl_src_module):
        if not isinstance(etl_src_module, str):
            return False

        # Additional validation logic for ETL_SRC_MODULE, if needed
        # Add your validation logic here

        return True

    def _validate_log_path(self, log_path):
        if not isinstance(log_path, str):
            return False

        # Additional validation logic for LOG_PATH, if needed
        # Add your validation logic here

        return True

    def _validate_log_prefix(self, log_prefix):
        if not isinstance(log_prefix, str):
            return False

        # Additional validation logic for LOG_PREFIX, if needed
        # Add your validation logic here

        return True

    def _validate_table_json_path(self, table_json_path):
        if not isinstance(table_json_path, str):
            return False

        # Additional validation logic for TABLE_JSON_PATH, if needed
        # Add your validation logic here

        return True

    def _validate_extractor_batch_size(self, extractor_batch_size):
        if not isinstance(extractor_batch_size, int) or extractor_batch_size <= 0:
            return False

        # Additional validation logic for EXTRACTOR_BATCH_SIZE, if needed
        # Add your validation logic here

        return True
