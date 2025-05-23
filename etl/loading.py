from typing import List, Dict, Any
# Models are now in etl.schemas
try:
    from .schemas import Patient, DeviceReading, ErrorRecord 
except ImportError: # Handle running script directly for testing or if path issues
    # Fallback simplified model definitions for standalone testing
    # These should ideally match the structure of those in schemas.py if used.
    from pydantic import BaseModel, Field as PydanticField # Use a different name for Field to avoid clash if any
    from typing import Optional as PyOptional, Any as PyAny
    class Patient(BaseModel): # type: ignore
        id: PyOptional[PyAny] = None
        name: str
        # Add other fields as necessary for the test examples if this fallback is hit
    class DeviceReading(BaseModel): # type: ignore
        reading_id: PyOptional[PyAny] = None
        timestamp: str
        # Add other fields
    class ErrorRecord(BaseModel): # type: ignore
        reference: PyAny
        error_type: str
        case_description: str
        # Add other fields


# In-memory storage for processed data and errors
processed_patients_storage: List[Patient] = [] # type: ignore 
processed_readings_storage: List[DeviceReading] = [] # type: ignore
error_records_storage: List[ErrorRecord] = [] # type: ignore

def load_data(
    patients: List[Patient], # type: ignore
    readings: List[DeviceReading] # type: ignore
    # In a real scenario, a database_connection or target configuration would be passed
) -> Dict[str, Any]:
    """
    Loads processed patient and device reading data into their respective in-memory stores.
    Simulates loading into a database or data warehouse.
    Returns a summary of loaded data.
    """
    global processed_patients_storage, processed_readings_storage
    
    loaded_patients_count = 0
    loaded_readings_count = 0
    loading_errors = [] # For errors during the loading process itself (e.g., DB connection)

    try:
        # Simulate loading patients
        for patient in patients:
            # In a real DB, this would be an INSERT or UPSERT operation
            processed_patients_storage.append(patient)
            loaded_patients_count += 1
        
        # Simulate loading device readings
        for reading in readings:
            processed_readings_storage.append(reading)
            loaded_readings_count += 1
            
    except Exception as e:
        # Catch any unexpected errors during the "loading" process
        loading_errors.append({
            "type": "DATA_LOADING_ERROR",
            "description": str(e),
            "item_count_patients": len(patients),
            "item_count_readings": len(readings)
        })
        # Depending on strategy, might clear partially loaded data or flag it

    return {
        "loaded_patients_count": loaded_patients_count,
        "loaded_readings_count": loaded_readings_count,
        "loading_errors": loading_errors,
        "total_patients_in_storage": len(processed_patients_storage),
        "total_readings_in_storage": len(processed_readings_storage)
    }

def load_error_data(errors: List[ErrorRecord]) -> Dict[str, Any]: # type: ignore
    """
    Loads structured error records into an in-memory "quarantine" store.
    Returns a summary of loaded error data.
    """
    global error_records_storage
    
    loaded_errors_count = 0
    
    try:
        for error_record in errors:
            error_records_storage.append(error_record)
            loaded_errors_count += 1
    except Exception as e:
        # This part is less likely to fail for in-memory lists but good practice for real DBs
        return {
            "loaded_errors_count": loaded_errors_count,
            "loading_error_detail": str(e),
            "total_errors_in_storage": len(error_records_storage)
        }

    return {
        "loaded_errors_count": loaded_errors_count,
        "total_errors_in_storage": len(error_records_storage)
    }

def get_all_loaded_data() -> Dict[str, List[Any]]:
    """Returns all data currently in the in-memory stores."""
    return {
        "patients": processed_patients_storage,
        "device_readings": processed_readings_storage,
        "errors": error_records_storage
    }

def clear_storage():
    """Clears all in-memory storage. Useful for testing."""
    global processed_patients_storage, processed_readings_storage, error_records_storage
    processed_patients_storage = []
    processed_readings_storage = []
    error_records_storage = []


if __name__ == '__main__':
    # Example Usage
    # The dummy Pydantic-like classes for standalone testing now need to align with etl.schemas
    # or this block should directly use `from .schemas import Patient, DeviceReading, ErrorRecord`
    # if this script is run as part of the package.
    try:
        from .schemas import Patient, DeviceReading, ErrorRecord
    except ImportError: # Fallback for running standalone, using the simplified fallbacks defined above
        # This assumes the fallback classes Patient, DeviceReading, ErrorRecord are already defined above
        # from pydantic import BaseModel, Field etc.
        pass


    # Sample data (simulating output from transformation step)
    # Ensure these samples are compatible with the schemas (full or fallback)
    sample_valid_patients = [
        Patient(id=1, name="Alice Wonderland", dob="1990-01-01", gender="F", address="1st St", email="a@e.com", phone="1", sex="F"), # type: ignore
        Patient(id=3, name="Charlie Brown", dob="1950-07-30", gender="M", address="2nd St", email="c@e.com", phone="2", sex="M") # type: ignore
    ]
    
    sample_valid_readings = [
        DeviceReading(reading_id="r1", patient_id=1, timestamp="2023-01-01T10:00:00Z", glucose=120.5), # type: ignore
        DeviceReading(reading_id="r2", patient_id=1, timestamp="2023-01-01T09:00:00Z", glucose=110.0) # type: ignore
    ]
    
    sample_error_records = [
        ErrorRecord(reference=2, error_type="INVALID_FORMAT", case_description="Invalid email", original_value="bob@", source_table="patients"), # type: ignore
        ErrorRecord(reference="r3", error_type="INVALID_TYPE", case_description="Glucose not a number", original_value="high", source_table="device_readings") # type: ignore
    ]

    print("--- Clearing Storage (for clean test run) ---")
    clear_storage()

    print("\n--- Loading Valid Data ---")
    load_summary = load_data(sample_valid_patients, sample_valid_readings)
    print(f"Load Summary: {load_summary}")

    print("\n--- Loading Error Data ---")
    error_load_summary = load_error_data(sample_error_records)
    print(f"Error Load Summary: {error_load_summary}")

    print("\n--- All Data in Storage ---")
    all_data = get_all_loaded_data()
    print(f"Stored Patients: {len(all_data['patients'])}")
    for p in all_data['patients']: print(p) # type: ignore
    
    print(f"\nStored Readings: {len(all_data['device_readings'])}")
    for r in all_data['device_readings']: print(r) # type: ignore
        
    print(f"\nStored Errors: {len(all_data['errors'])}")
    for e in all_data['errors']: print(e) # type: ignore

    print("\n--- Attempting to load empty lists ---")
    empty_load_summary = load_data([], [])
    print(f"Empty Load Summary: {empty_load_summary}")
    empty_error_summary = load_error_data([])
    print(f"Empty Error Load Summary: {empty_error_summary}")
    
    all_data_after_empty = get_all_loaded_data()
    assert len(all_data_after_empty['patients']) == len(all_data['patients']) # Ensure no data loss
    assert len(all_data_after_empty['errors']) == len(all_data['errors'])

    print("\n--- Test clearing storage again ---")
    clear_storage()
    all_data_cleared = get_all_loaded_data()
    assert len(all_data_cleared['patients']) == 0
    assert len(all_data_cleared['device_readings']) == 0
    assert len(all_data_cleared['errors']) == 0
    print("Storage cleared successfully.")
