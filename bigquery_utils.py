from google.cloud import bigquery
import json


class BigqueryTable:
    def __init__(self, config_file: json):
        with open(config_file, 'r') as f:
            self.config_file = json.load(f)
            self.key_path = self.config_file["KEY"]
            self.table_id = self.config_file["TABLE_ID"]
        self.client = self.get_client()

    def get_client(self) -> bigquery.Client:
        client = bigquery.Client.from_service_account_json(self.key_path)
        return client

    def create_bq_table(self, schema_file: json) -> None:
        with open(schema_file, 'r') as f:
            schema = json.load(f)
        table_config = bigquery.Table(self.table_id, schema=schema)
        self.client.create_table(table_config, exists_ok=True)


