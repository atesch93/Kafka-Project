from datetime import datetime
import requests

class Hystreet_API:
    def __init__(self,city ,API_key):
        self.API_key = API_key
        self.city = city

    def get_pdstrian_info(self):
        url = f'https://hystreet.com/api/{self.city}/{id}'
        response = requests.get(url)
        json_response = response.json()
        street_name = json_response["name"]
        pedestrian_num = json_response["today_count"]
        measure_date = json_response["metadata"]["measured_from"]
        measure_date = datetime(measure_date)
        base_date = datetime(1979, 1, 1)
        ts = int((measure_date - base_date).total_seconds())
        return (street_name,pedestrian_num,measure_date,ts)
