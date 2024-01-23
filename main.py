import requests
import json
import csv
from datetime import datetime, timedelta
import os

def load_config():
    try:
        with open("config.json", "r") as config_file:
            config = json.load(config_file)
            return config
    except FileNotFoundError:
        print("Error: config.json file not found.")
        return None

def load_sensor_indices_from_csv(csv_file):
    try:
        with open(csv_file, newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            if 'sensor_index' not in reader.fieldnames:
                raise ValueError("Error: 'sensor_index' column not found in sensors CSV.")
            return [row['sensor_index'] for row in reader]
    except FileNotFoundError:
        print(f"Error: {csv_file} not found.")
        return []
    except Exception as e:
        print(f"Error: {e}")
        return []

def get_sensor_history(api_key, sensor_index, start_timestamp, end_timestamp, average_minutes, fields):
    base_url = f"https://api.purpleair.com/v1/sensors/{sensor_index}/history"
    headers = {
        "X-API-Key": api_key,
    }
    params = {
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        "average": average_minutes,
        "fields": fields,
    }
    response = requests.get(base_url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None

def format_timestamp(timestamp):
    return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')

def save_to_csv(sensor_index, start_date, end_date, data):
    folder_name = f"{start_date}_{end_date}"
    os.makedirs(folder_name, exist_ok=True)

    if not data:
        csv_filename = f"empty_{sensor_index}.csv"
    else:
        csv_filename = f"{sensor_index}.csv"

    csv_path = os.path.join(folder_name, csv_filename)
    with open(csv_path, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['Date', 'PM2.5_1week'])
        for data_point in data:
            timestamp, pm25_value = data_point
            formatted_date = format_timestamp(timestamp)
            csv_writer.writerow([formatted_date, pm25_value])

if __name__ == "__main__":
    # Load API key from config.json
    config = load_config()
    if not config:
        exit()

    api_key = config.get("api_key")

    # Load sensor indices from sensors.csv
    sensor_indices = load_sensor_indices_from_csv("sensors.csv")

    # Take input for start date and end date from the command line
    start_date_str = input("Enter the start date (YYYY-MM-DD): ")
    end_date_str = input("Enter the end date (YYYY-MM-DD): ")

    try:
        # Convert input strings to datetime objects
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d") + timedelta(days=7)
    except ValueError:
        print("Error: Invalid date format. Use YYYY-MM-DD.")
        exit()

    # Convert dates to UNIX timestamps
    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())

    # Set the desired average (in minutes) and fields
    average_minutes = 10080  # 1 week
    fields = "pm2.5_alt"

    # Iterate over each sensor index and get sensor history
    for sensor_index in sensor_indices:
        result = get_sensor_history(api_key, sensor_index, start_timestamp, end_timestamp, average_minutes, fields)
        if result:
            print(f"Sensor {sensor_index} History:")
            sorted_data = sorted(result['data'], key=lambda x: x[0])  # Sort by timestamp
            for data_point in sorted_data:
                timestamp, pm25_value = data_point
                formatted_date = format_timestamp(timestamp)
                print(f"Date: {formatted_date}, PM2.5_1week: {pm25_value}")

            save_to_csv(sensor_index, start_date_str, end_date_str, sorted_data)
            print(f"Data saved to CSV file.\n")
        else:
            print(f"No data available for Sensor {sensor_index}.\n")
