import csv
import requests
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage
import json

"""
module for main cloud function to retrieve raw data, transform it and write it into bucket and bigquery table
"""


class RatesHandler:
    def __init__(self):
        self.config = self.get_config()
        self.symbols = self.config['symbols']
        self.base = self.config['base']
        self.access_key = self.config['access_key']  # api access key

    @staticmethod
    def get_config():
        """
        reads file from bucket to use it as a config file
        :return: dict: a dictionary representing key values to use in cloud functions
        """
        storage_client = storage.Client.from_service_account_json(
            "/home/kote/Downloads/historical-exchange-rate-83cbcd633a7c.json")
        bucket = storage_client.get_bucket("store_config_bucket1")
        filename = "config_file.json"
        blob = bucket.get_blob(filename)
        file_data = blob.download_as_string()
        out_file = json.loads(file_data)
        return out_file

    def get_api(self):
        """
        requests api from exchangeratesapi
        :return:
        file:A file object representing the API response.
        """
        try:
            # symbols = ["GBP", "JPY", "USD", "GEL", "OMR"]
            url = f"http://api.exchangeratesapi.io/v1/latest?base=" \
                  f"{self.base}&symbols={','.join(self.symbols)}" \
                  f"&access_key={self.access_key}"
            response = requests.get(url)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print("error", e)

    def convert_data_to_csv(self):
        """
        converts data from api into a csv file
        :return:
        file: a file object representing csv file
        """
        try:
            response = self.get_api()
            data = response.json()["rates"]
            now = datetime.now()
            timestamp = now.strftime("%Y-%m-%d-%H")
            output_file = f"rates_{timestamp}.csv"
            currencies = ["GBP", "JPY", "USD", "GEL", "OMR"]
            rows = []
            for currency in currencies:
                row = {
                    "date": timestamp,
                    "currency": currency,
                    "rate": data[currency]
                }
                rows.append(row)

            fieldnames = rows[0].keys()
            with open(output_file, "w", newline="") as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            return output_file
        except Exception as e:
            print(f"Failed to convert json into csv: ", str(e))

    def write_csv_to_buket(self):
        """ writes csv file to a gcp bucket """

        try:
            file_name = self.convert_data_to_csv()
            client = storage.Client.from_service_account_json(self.config['KEY'])
            bucket = client.get_bucket(self.config['BUCKET_NAME'])
            blob = bucket.blob(file_name)
            blob.upload_from_filename(file_name)
        except Exception as e:
            print(f"to write to bucket", str(e))

    def write_to_bq_table(self):
        """ writes csv file to a bigquery table from a bucket """
        try:
            with open("schema.json", "r") as f:
                schema = json.load(f)
            client = bigquery.Client.from_service_account_json(self.config['KEY'])
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                skip_leading_rows=1,
                source_format=bigquery.SourceFormat.CSV,
            )
            load_job = client.load_table_from_uri(
                "gs://{}/{}".format(self.config['BUCKET_NAME'], self.convert_data_to_csv()),
                self.config['TABLE_ID'],
                job_config=job_config)

            load_job.result()
            if load_job.state == 'DONE':
                print('Data loaded successfully.')
            else:
                print('Error loading data:', load_job.errors)
        except Exception as e:
            print(f"Failed to load data into table", {str(e)})
