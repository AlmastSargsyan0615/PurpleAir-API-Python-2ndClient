import requests
import json
import csv
import os
from datetime import datetime, timedelta
import pandas as pd

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

def save_to_excel(sensor_index, start_date, end_date, data):
    folder_name = f"{start_date}_{end_date}"
    os.makedirs(folder_name, exist_ok=True)

    if not data:
        excel_filename = f"empty_{sensor_index}.xlsx"
    else:
        excel_filename = f"{sensor_index}.xlsx"

    excel_path = os.path.join(folder_name, excel_filename)
    df = pd.DataFrame(data, columns=['Date'] + [f'{field}' for field in data[0][1]])
    df.to_excel(excel_path, index=False)
    print(f"Data saved to Excel file: {excel_path}")

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
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    except ValueError:
        print("Error: Invalid date format. Use YYYY-MM-DD.")
        exit()

    # Convert dates to UNIX timestamps
    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())

    # Set the desired average (in minutes) and fields
    average_minutes = 10080  # 1 week
    fields = "latitude,longitude,temperature,humidity,pm2.5_atm"

    # Iterate over each sensor index and get sensor history
    for sensor_index in sensor_indices:
        result = get_sensor_history(api_key, sensor_index, start_timestamp, end_timestamp, average_minutes, fields)
        if result:
            print(f"Sensor {sensor_index} History:")
            sorted_data = sorted(result['data'], key=lambda x: x[0])  # Sort by timestamp

            # Extract the fields from the first data point
            field_names = ['Date'] + [field for timestamp, values in sorted_data for field in values.keys()]
            data = [[''] * len(field_names) for _ in range(len(sorted_data))]

            # Populate the data matrix
            for i, (timestamp, values) in enumerate(sorted_data):
                formatted_date = format_timestamp(timestamp)
                data[i][0] = formatted_date
                for j, field in enumerate(values.keys(), start=1):
                    data[i][j] = values[field]

            save_to_excel(sensor_index, start_date_str, end_date_str, data)
            print("\n")
        else:
            print(f"No data available for Sensor {sensor_index}.\n")
