import json
import requests
from redis import Redis
from redis.exceptions import ResponseError
from db_config import get_redis_connection

class api_functions:
    def __init__(self):
        self.r = get_redis_connection()

    def fetch_data_from_api(self):
        """ Takes the data from api in json format
             if any error occur it will return exception.
        """
        api_url = "https://www.freetogame.com/api/games?platform=pc"
        response = requests.get(api_url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Issue with fetching data from api: {response.status_code}")

    def insert_into_redis(self, data):
        """
        In this function we will delete the existing data and insert
        new data into Redis.
        """
        self.delete_existing_data()
        for index, item in enumerate(data):
            key = f"Object:{index}"
            try:
                self.r.execute_command('JSON.SET', key, '.', json.dumps(item))
            except ResponseError as e:
                print(f"Error inserting data into Redis: {e}")
                continue

        return len(data)  
    
    def delete_existing_data(self):
        """
        This function tries to delete the data with key name as Object.
        """
        keys = self.r.keys("Object:*")
        for key in keys:
            self.r.delete(key)

    def check_data_inserted(self, num_items):
        """
        this function tries to check whether data inserted or not.
        """
        inserted_items = 0
        for index in range(num_items):
            key = f"Object:{index}"
            if self.r.exists(key):
                inserted_items += 1
        return inserted_items
    
    def fetch_data_from_redis(self):
        """
        this function tries to fetch data from Redis.
        """
        data = []
        try:
            keys = self.r.keys("Object:*")  
            for key in keys:
                raw_data = self.r.execute_command('JSON.GET', key)
                data.append(json.loads(raw_data))
            return data
        except Exception as e:
            print(f"Error retrieving  data from Redis: {e}")
        return None

    def print_first_5_rows(self, data):
        """
        This function tries to print first 5 records.
        """
        for index, item in enumerate(data[:5]):
            print(f"Row {index + 1}: {item}")
