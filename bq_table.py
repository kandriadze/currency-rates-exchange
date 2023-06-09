from bigquery_utils import BigqueryTable

table = BigqueryTable("config.json")
table.get_client()
table.create_bq_table("schema.json")