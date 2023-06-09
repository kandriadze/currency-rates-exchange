import json
import requests
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime


def load_config():
    storage_client = storage.Client.from_service_account_json(
        "/home/kote/Downloads/historical-exchange-rate-83cbcd633a7c.json")
    bucket = storage_client.get_bucket("store_config_bucket1")
    filename = "config_file.json"
    blob = bucket.get_blob(filename)
    file_data = blob.download_as_string()
    config = json.loads(file_data)
    return config


def launcher():
    try:
        config_file = load_config()
        client = bigquery.Client.from_service_account_json(config_file['KEY'])
        current_time = datetime.now()
        curr_date = current_time.strftime("%Y-%m-%d-%H")
        query = f"SELECT count(*) from '{config_file['TABLE_ID']}' WHERE extract(Date from Date)={curr_date}"
        query_job = client.query(query)
        result = query_job.result()
        row_count = next(result)[0]

        if row_count > 0:
            return 'Data has already been retrieved for the current hour'
        else:
            response = requests.get(config_file['MAIN_FUN_URL'])
            if response.status_code == 200:
                return 'Main Cloud Function triggered successfully'
            else:
                return f'Failed to trigger Main Cloud Function: {response.text}'
    except Exception as e:
        print("Failed to trigger main function:", str(e))
