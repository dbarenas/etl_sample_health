import json
import csv

def extract_json(filepath: str) -> list[dict]:
    """Extracts data from a JSON file."""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        print(f"Error: File not found at {filepath}")
        return []
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {filepath}")
        return []

def extract_csv(filepath: str) -> list[dict]:
    """Extracts data from a CSV file."""
    try:
        with open(filepath, 'r', newline='') as f:
            reader = csv.DictReader(f)
            data = list(reader)
        return data
    except FileNotFoundError:
        print(f"Error: File not found at {filepath}")
        return []
    except IOError:
        print(f"Error: An I/O error occurred while reading {filepath}")
        return []

def extract_data(patient_filepath: str, device_filepath: str, patient_file_type: str = 'json', device_file_type: str = 'csv') -> tuple[list[dict], list[dict]]:
    """
    Orchestrates reading both patient and device data from their respective files.
    Allows specifying file types, defaulting to JSON for patients and CSV for devices.
    """
    patient_data = []
    device_data = []

    if patient_file_type.lower() == 'json':
        patient_data = extract_json(patient_filepath)
    elif patient_file_type.lower() == 'csv':
        patient_data = extract_csv(patient_filepath)
    else:
        print(f"Unsupported file type for patient data: {patient_file_type}")

    if device_file_type.lower() == 'csv':
        device_data = extract_csv(device_filepath)
    elif device_file_type.lower() == 'json':
        device_data = extract_json(device_filepath)
    else:
        print(f"Unsupported file type for device data: {device_file_type}")
        
    return patient_data, device_data

if __name__ == '__main__':
    # Example Usage (assuming you have dummy files in a 'data' directory)
    # Create dummy files for testing if they don't exist
    try:
        # Corrected paths for execution from root
        with open('data/patients.json', 'w') as f:
            json.dump([{"name": "John Doe", "id": 1}], f)
        with open('data/device_readings.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["patient_id", "reading"])
            writer.writerow([1, "120/80"])
    except IOError:
        print("Could not create dummy files for example usage.")

    # Corrected paths for execution from root
    patients, devices = extract_data('data/patients.json', 'data/device_readings.csv')
    print("Patients:", patients)
    print("Devices:", devices)

    # Example with CSV for patients and JSON for devices (if you had such files)
    # patients_csv, devices_json = extract_data('data/patients.csv', 'data/devices.json', patient_file_type='csv', device_file_type='json')
    # print("Patients (CSV):", patients_csv)
    # print("Devices (JSON):", devices_json)
