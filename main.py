import asyncio
import time
from etl.extraction import extract_data
from etl.transformation import pipeline_transform
from etl.loading import load_data, load_error_data, clear_storage, get_all_loaded_data
import os
import json
import csv

# --- Sample Data Creation ---
def create_sample_data_files():
    """Creates sample JSON and CSV files in the 'data' directory if they don't exist."""
    if not os.path.exists('data'):
        os.makedirs('data')

    patients_json_path = 'data/patients.json'
    device_readings_csv_path = 'data/device_readings.csv'

    if not os.path.exists(patients_json_path):
        sample_patients = [
            {"id": 1, "name": "Alice Wonderland", "dob": "1990-01-01", "gender": "Female", "address": "123 Main St", "email": "alice@example.com", "phone": "555-1234", "sex": "Female"},
            {"id": 2, "name": "Bob The Builder", "dob": "03/15/1985", "gender": "Male", "address": "456 Side St", "email": "bob@", "phone": "555-5678", "sex": "Male"},
            {"id": 3, "name": "Charlie Brown", "dob": "1950-07-30", "gender": "MALE", "address": "789 Other St", "email": "charlie@goodgrief.com", "phone": "invalid-phone", "sex": "male"},
            {"name": "Missing ID", "dob": "2000-01-01", "gender": "Non-binary", "address": "N/A", "email": "test@example.com", "phone": "1234567890", "sex": "Non-binary"},
            {"id": 4, "name": "Invalid Date", "dob": "1990/01/01", "gender": "Female", "address": "Error Lane", "email": "error@example.com", "phone": "555-0000", "sex": "Female"},
        ]
        with open(patients_json_path, 'w') as f:
            json.dump(sample_patients, f, indent=2)
        print(f"Created sample data: {patients_json_path}")

    if not os.path.exists(device_readings_csv_path):
        sample_readings_header = ["reading_id", "patient_id", "timestamp", "glucose", "systolic_bp", "diastolic_bp", "weight"]
        sample_readings_data = [
            ["r1", "1", "2023-01-01T10:00:00Z", "120.5", "120", "80", "150.0"],
            ["r2", "1", "2023-01-01T09:00:00Z", "110.0", "118", "78", "150.5"], # Timestamp order issue for patient 1
            ["r3", "2", "2023-01-02T12:00:00Z", "high", "140", "90", "200.0"],   # Invalid glucose type
            ["r4", "2", "2023-01-02T14:00:00Z", "99.0", "130", "150", "198.0"],  # Diastolic > Systolic
            ["r5", "3", "invalid_timestamp", "100.0", "120", "80", "160.0"],      # Invalid timestamp
            ["r6", "3", "2023-01-03T10:00:00Z", "5000", "125", "75", "unknown"], # Glucose outlier, weight type
            ["r7", "", "2023-01-04T10:00:00Z", "105", "122", "82", "165.0"], # Missing patient_id (if considered)
        ]
        with open(device_readings_csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(sample_readings_header)
            writer.writerows(sample_readings_data)
        print(f"Created sample data: {device_readings_csv_path}")

# --- Asynchronous Pipeline Functions ---
async def extract_data_async(patient_filepath: str, device_filepath: str):
    print("Starting data extraction...")
    # In a real-world scenario with I/O bound operations,
    # you might use libraries like aiofiles for true async file reading.
    # For this example, we'll run the synchronous extract_data in a thread pool
    # to avoid blocking the asyncio event loop if it were doing CPU-bound work.
    # However, since extract_data is simple and mostly I/O, it might not show huge benefits here
    # without actual async file I/O.
    loop = asyncio.get_event_loop()
    start_time = time.time()
    # Using default executor (ThreadPoolExecutor)
    patient_data, device_data = await loop.run_in_executor(
        None, extract_data, patient_filepath, device_filepath
    )
    duration = time.time() - start_time
    print(f"Extraction completed in {duration:.2f} seconds. Patients: {len(patient_data)}, Devices: {len(device_data)}")
    return patient_data, device_data

async def transform_data_async(patient_data, device_data):
    print("Starting data transformation...")
    # Transformation can be CPU-bound. Running in executor if it's complex.
    loop = asyncio.get_event_loop()
    start_time = time.time()
    processed_patients, processed_readings, error_records = await loop.run_in_executor(
        None, pipeline_transform, patient_data, device_data
    )
    duration = time.time() - start_time
    print(f"Transformation completed in {duration:.2f} seconds. Valid Patients: {len(processed_patients)}, Valid Readings: {len(processed_readings)}, Errors: {len(error_records)}")
    return processed_patients, processed_readings, error_records

async def load_data_async(processed_patients, processed_readings, error_records):
    print("Starting data loading...")
    # Loading (even to in-memory) is generally quick but shown as async for consistency
    loop = asyncio.get_event_loop()
    start_time = time.time()
    
    # These are synchronous functions, running them in executor for pattern consistency
    # or if they were to become I/O bound (e.g. writing to a real database)
    load_summary = await loop.run_in_executor(None, load_data, processed_patients, processed_readings)
    error_load_summary = await loop.run_in_executor(None, load_error_data, error_records)
    
    duration = time.time() - start_time
    print(f"Loading completed in {duration:.2f} seconds.")
    return load_summary, error_load_summary

# --- Main Orchestrator ---
async def main_async():
    """Orchestrates the ETL pipeline asynchronously."""
    start_total_time = time.time()

    # Ensure sample data exists for the pipeline to run
    create_sample_data_files()
    
    # Define file paths (could be from config)
    patient_json_file = 'data/patients.json'
    device_csv_file = 'data/device_readings.csv'

    # Clear storage from previous runs (for this example)
    print("Clearing previous data from in-memory storage...")
    clear_storage()

    # 1. Extraction
    # We can run extractions for different sources in parallel if they were independent
    # For now, extract_data handles both, so it's one async step.
    raw_patients, raw_devices = await extract_data_async(patient_json_file, device_csv_file)

    if not raw_patients and not raw_devices:
        print("No data extracted. Exiting pipeline.")
        return

    # 2. Transformation
    # This step depends on the output of extraction.
    valid_patients, valid_readings, errors = await transform_data_async(raw_patients, raw_devices)

    # 3. Loading
    # Loading can happen once transformation is complete.
    # Loading valid data and error data can be done concurrently.
    load_task = load_data_async(valid_patients, valid_readings, errors)
    
    # Awaiting the load_task which internally calls load_data and load_error_data
    load_summary, error_load_summary = await load_task
    
    end_total_time = time.time()
    total_duration = end_total_time - start_total_time

    print("\n--- ETL Pipeline Summary ---")
    print(f"Total execution time: {total_duration:.2f} seconds")
    print(f"Patients extracted: {len(raw_patients)}")
    print(f"Device readings extracted: {len(raw_devices)}")
    print(f"Valid patients processed: {len(valid_patients)}")
    print(f"Valid device readings processed: {len(valid_readings)}")
    print(f"Error records generated: {len(errors)}")
    
    print("\n--- Loading Summary ---")
    print(f"Successfully loaded patients: {load_summary.get('loaded_patients_count', 0)}")
    print(f"Successfully loaded device readings: {load_summary.get('loaded_readings_count', 0)}")
    if load_summary.get('loading_errors'):
        print(f"Data loading errors: {load_summary['loading_errors']}")
    
    print(f"Successfully loaded error records: {error_load_summary.get('loaded_errors_count', 0)}")
    if error_load_summary.get('loading_error_detail'):
        print(f"Error data loading error: {error_load_summary['loading_error_detail']}")

    # You can optionally print the loaded data
    # final_data = get_all_loaded_data()
    # print("\n--- Data in Storage ---")
    # print(f"Total Patients in Storage: {len(final_data['patients'])}")
    # print(f"Total Readings in Storage: {len(final_data['device_readings'])}")
    # print(f"Total Errors in Storage: {len(final_data['errors'])}")


if __name__ == "__main__":
    print("Running ETL Pipeline...")
    asyncio.run(main_async())
    print("ETL Pipeline finished.")
