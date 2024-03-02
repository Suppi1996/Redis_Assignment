import json
from db_config import get_redis_connection 
from datetime import datetime

class Analysis:
    def __init__(self):
        self.r = get_redis_connection()

    def fetch_data_from_redis(self):
        """
        This function fetches the data from Redis.
        """
        all_json_data = []
        try:
            keys = self.r.keys("Object:*")  
            for key in keys:
                json_data = self.r.execute_command('JSON.GET', key)
                all_json_data.append(json.loads(json_data))
            return all_json_data
        except Exception as e:
            print(f"Error retrieving JSON data from Redis: {e}")
        return None


    def find_most_common_publisher(self):
        """
        This function finds the most common publisher.
        """
        data=self.fetch_data_from_redis()
        publisher_counts = {}
        for row in data:
            publisher = row.get('publisher')
            if publisher:
                publisher_counts[publisher] = publisher_counts.get(publisher, 0) + 1
        most_common_publisher = max(publisher_counts, key=publisher_counts.get)
        return most_common_publisher


    def calculate_average_short_description_length(self):
        """
        Calculate the average length of short descriptions for all games.
        """
        data = self.fetch_data_from_redis()
        total_length = sum(len(row.get('short_description')) for row in data)
        return total_length / len(data) if data else 0

    def publisher_developer_relationship(self):
        """
        This function how many games have same publisher and developer.
        """
        data=self.fetch_data_from_redis()
        same_publisher_developer = 0
        for row in data:
            publisher = row.get('publisher')
            developer = row.get('developer')
            if publisher == developer:
                same_publisher_developer += 1
        return same_publisher_developer


