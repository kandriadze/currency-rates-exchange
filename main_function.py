from main_utils import RatesHandler

'''functions to call class methods from main_utils.py'''


def main_function():
    obj = RatesHandler()
    obj.get_api()
    obj.get_config()
    obj.convert_data_to_csv()
    obj.write_csv_to_buket()
    obj.write_to_bq_table()
