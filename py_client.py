import requests
import time

# Base URL of the Rust server
BASE_URL = "http://localhost:8000"

# Function to insert telemetry data into the Rust database
def insert_telemetry(sensor_name, timestamp, value, timeseries_id, fc1_flag=None):
    url = f"{BASE_URL}/telemetry"
    data = {
        "sensor_name": sensor_name,
        "timestamp": timestamp,
        "value": value,
        "fc1_flag": fc1_flag,
        "timeseries_id": timeseries_id
    }
    response = requests.post(url, json=data)
    if response.status_code == 200:
        print(f"Data inserted successfully: {data}")
    else:
        print(f"Failed to insert data: {response.status_code}, {response.text}")

# Function to query telemetry data from the Rust database
def query_telemetry(timeseries_id, start_time, end_time):
    url = f"{BASE_URL}/query_by_id"
    params = {
        "timeseries_id": timeseries_id,
        "start_time": start_time,
        "end_time": end_time
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        print("Query successful. Data received:")
        return response.json()
    else:
        print(f"Failed to query data: {response.status_code}, {response.text}")
        return None

# Function to check for a fault based on fan speed exceeding a threshold
def check_for_fault(data, fault_threshold=0.95):
    fault_count = 0
    for entry in data:
        if entry['value'] > fault_threshold:
            fault_count += 1
            print(f"Fault detected at timestamp {entry['timestamp']} with value {entry['value']}")
    if fault_count == 0:
        print("No faults detected.")
    else:
        print(f"Total faults detected: {fault_count}")

# Example usage
if __name__ == "__main__":
    # Insert some telemetry data
    insert_telemetry("Sa_FanSpeed", "2024-08-28T12:00:00Z", 0.8, "8f541ba4-c437-43ba-ba1d-5c946583fe54")
    insert_telemetry("Sa_FanSpeed", "2024-08-28T12:01:00Z", 0.9, "8f541ba4-c437-43ba-ba1d-5c946583fe54")
    insert_telemetry("Sa_FanSpeed", "2024-08-28T12:02:00Z", 1.0, "8f541ba4-c437-43ba-ba1d-5c946583fe54")

    # Query the data for the last 3 minutes
    start_time = "2024-08-28T12:00:00Z"
    end_time = "2024-08-28T12:03:00Z"
    data = query_telemetry("8f541ba4-c437-43ba-ba1d-5c946583fe54", start_time, end_time)

    # Check for faults in the queried data
    if data:
        check_for_fault(data, fault_threshold=0.95)
