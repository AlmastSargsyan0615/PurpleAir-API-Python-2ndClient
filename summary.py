import csv
from collections import defaultdict
import math

def calculate_average(data):
    if data:
        return sum(data) / len(data)
    else:
        return None

def generate_summary(input_filename, sensors_filename, output_filename):
    sensor_data = defaultdict(list)

    # Read data from detail.csv
    with open(input_filename, mode='r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            sensor_index = row['Sensor']
            pm25_value = float(row['PM2.5_1week'])
            sensor_data[sensor_index].append(pm25_value)

    # Read sensor indices from sensors.csv in order
    with open(sensors_filename, newline='', encoding='utf-8-sig') as sensorfile:
        sensor_reader = csv.DictReader(sensorfile)
        all_sensor_indices = [row['sensor_index'] for row in sensor_reader]

    # Include all sensor indices in the summary
    for sensor_index in all_sensor_indices:
        if sensor_index not in sensor_data:
            sensor_data[sensor_index] = []

    # Write summary to summary.csv with ordered sensor indices
    with open(output_filename, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["sensor_index", "weeks_count", "average_PM2.5_1week"])

        for sensor_index in all_sensor_indices:
            pm25_values = sensor_data[sensor_index]
            weeks_count = len(pm25_values)
            average_pm25 = calculate_average(pm25_values)
            writer.writerow([sensor_index, weeks_count, average_pm25])

if __name__ == "__main__":
    input_file = "detail.csv"
    sensors_file = "sensors.csv"
    output_file = "summary.csv"

    generate_summary(input_file, sensors_file, output_file)
    print(f"Summary has been written to {output_file}.")
