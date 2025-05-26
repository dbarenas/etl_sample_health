# main.py
import asyncio
import time
from etl.extraction import extract_data
from etl.transformation import pipeline_transform
# Updated imports for loading and db_utils
from etl.loading import initialize_database_schema, load_data, load_error_data 
from etl.db_utils import get_db_connection # Make sure this import is correct
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
            {"id": "p1", "name": "Alice Wonderland", "dob": "1990-01-01", "gender": "Female", "address": "123 Main St", "email": "alice@example.com", "phone": "555-1234", "sex": "Female"},
            {"id": "p2", "name": "Bob The Builder", "dob": "03/15/1985", "gender": "Male", "address": "456 Side St", "email": "bob@", "phone": "555-5678", "sex": "Male"},
            {"id": "p3", "name": "Charlie Brown", "dob": "1950-07-30", "gender": "MALE", "address": "789 Other St", "email": "charlie@goodgrief.com", "phone": "invalid-phone", "sex": "male"},
            {"id": "p4", "name": "Diana Prince", "dob": "2000-01-01", "gender": "Non-binary", "address": "N/A", "email": "diana@example.com", "phone": "1234567890", "sex": "Non-binary"}, # Valid record
            {"id": "p5", "name": "Invalid Date Man", "dob": "1990/01/01", "gender": "Male", "address": "Error Lane", "email": "error@example.com", "phone": "555-0000", "sex": "Male"},
        ]
        with open(patients_json_path, 'w') as f:
            json.dump(sample_patients, f, indent=2)
        print(f"Created sample data: {patients_json_path}")

    if not os.path.exists(device_readings_csv_path):
        sample_readings_header = ["id", "patient_id", "timestamp", "glucose", "systolic_bp", "diastolic_bp", "weight"] # Changed reading_id to id
        sample_readings_data = [
            ["r1", "p1", "2023-01-01T10:00:00Z", "120.5", "120", "80", "150.0"],
            ["r2", "p1", "2023-01-01T09:00:00Z", "110.0", "118", "78", "150.5"], 
            ["r3", "p2", "2023-01-02T12:00:00Z", "high", "140", "90", "200.0"],   
            ["r4", "p2", "2023-01-02T14:00:00Z", "99.0", "130", "150", "198.0"],  
            ["r5", "p3", "invalid_timestamp", "100.0", "120", "80", "160.0"],      
            ["r6", "p3", "2023-01-03T10:00:00Z", "5000", "125", "75", "unknown"], 
            ["r7", "p4", "2023-01-04T10:00:00Z", "105", "122", "82", "165.0"], 
        ]
        with open(device_readings_csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(sample_readings_header)
            writer.writerows(sample_readings_data)
        print(f"Created sample data: {device_readings_csv_path}")


# --- Asynchronous Pipeline Functions ---
async def extract_data_async(patient_filepath: str, device_filepath: str):
    print("Starting data extraction...")
    loop = asyncio.get_event_loop()
    start_time = time.time()
    patient_data, device_data = await loop.run_in_executor(
        None, extract_data, patient_filepath, device_filepath
    )
    duration = time.time() - start_time
    print(f"Extraction completed in {duration:.2f} seconds. Patients: {len(patient_data)}, Devices: {len(device_data)}")
    return patient_data, device_data

async def transform_data_async(patient_data, device_data):
    print("Starting data transformation...")
    loop = asyncio.get_event_loop()
    start_time = time.time()
    processed_patients, processed_readings, error_records = await loop.run_in_executor(
        None, pipeline_transform, patient_data, device_data
    )
    duration = time.time() - start_time
    print(f"Transformation completed in {duration:.2f} seconds. Valid Patients: {len(processed_patients)}, Valid Readings: {len(processed_readings)}, Errors: {len(error_records)}")
    return processed_patients, processed_readings, error_records

async def load_data_async(db_conn, processed_patients, processed_readings, error_records): # Added db_conn
    print("Starting data loading into database...")
    loop = asyncio.get_event_loop()
    start_time = time.time()
    
    load_summary = await loop.run_in_executor(None, load_data, db_conn, processed_patients, processed_readings)
    error_load_summary = await loop.run_in_executor(None, load_error_data, db_conn, error_records)
    
    duration = time.time() - start_time
    print(f"Database loading completed in {duration:.2f} seconds.")
    return load_summary, error_load_summary

# --- Main Orchestrator ---
async def main_async():
    start_total_time = time.time()
    db_conn = None 

    try:
        # Initialize database schema (creates tables if they don't exist)
        initialize_database_schema() # This handles its own connection

        # Establish a database connection for data loading operations
        db_conn = get_db_connection()
        if not db_conn:
            print("FATAL: Could not establish database connection. Exiting pipeline.")
            return

        create_sample_data_files() # Ensure sample data is present
        patient_json_file = 'data/patients.json'
        device_csv_file = 'data/device_readings.csv'

        raw_patients, raw_devices = await extract_data_async(patient_json_file, device_csv_file)

        if not raw_patients and not raw_devices:
            print("No data extracted. Exiting pipeline.")
            return

        valid_patients, valid_readings, errors = await transform_data_async(raw_patients, raw_devices)
        
        load_summary, error_load_summary = await load_data_async(db_conn, valid_patients, valid_readings, errors)
        
        end_total_time = time.time()
        total_duration = end_total_time - start_total_time

        print("\n--- ETL Pipeline Summary ---")
        print(f"Total execution time: {total_duration:.2f} seconds")
        print(f"Patients extracted: {len(raw_patients)}")
        print(f"Device readings extracted: {len(raw_devices)}")
        print(f"Valid patients for DB: {len(valid_patients)}") # Renamed for clarity
        print(f"Valid device readings for DB: {len(valid_readings)}") # Renamed for clarity
        print(f"Transformation error records: {len(errors)}") # Renamed for clarity
        
        print("\n--- Database Loading Summary ---")
        print(f"Successfully loaded patients to DB: {load_summary.get('loaded_patients_count', 0)}")
        print(f"Successfully loaded device readings to DB: {load_summary.get('loaded_readings_count', 0)}")
        if load_summary.get('db_loading_errors'):
            print("Database Loading Errors (Data):")
            for err in load_summary['db_loading_errors']:
                print(f"  - Type: {err.get('type')}, Ref: {err.get('reference')}, Desc: {err.get('description')}")
        
        print(f"Successfully loaded transformation error records to DB: {error_load_summary.get('loaded_errors_count', 0)}")
        if error_load_summary.get('db_error_loading_errors'):
            print("Database Loading Errors (Error Records):")
            for err in error_load_summary['db_error_loading_errors']:
                print(f"  - Type: {err.get('type')}, Ref: {err.get('reference')}, Desc: {err.get('description')}")
                
    except Exception as e:
        print(f"An unexpected error occurred in the main pipeline: {e}")
    finally:
        if db_conn:
            db_conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    print("Running ETL Pipeline with PostgreSQL Integration...")
    asyncio.run(main_async())
    print("ETL Pipeline finished.")
```
