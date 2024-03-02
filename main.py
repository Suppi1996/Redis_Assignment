from fetchapi import api_functions
from processing_func import Analysis

if __name__ == "__main__":
    api_calls = api_functions()
    data = api_calls.fetch_data_from_api()
    num_items_inserted = api_calls.insert_into_redis(data)
    print(f"{num_items_inserted} items inserted into Redis.")
    num_items_found = api_calls.check_data_inserted(num_items_inserted)
    print(f"{num_items_found} items found in Redis.")
    data = api_calls.fetch_data_from_redis()
    api_calls.print_first_5_rows(data)
    analysis = Analysis()
    common = analysis.find_most_common_publisher()
    len=analysis.calculate_average_short_description_length()
    publisher_developer_relationship = analysis.publisher_developer_relationship()
    print("Most common publisher:", common)
    print("Average Short description length:", len)
    print("Publisher Developer Relationship:", publisher_developer_relationship)

