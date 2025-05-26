import psycopg2 # For specific error types like IntegrityError
from .schemas import Patient, DeviceReading, ErrorRecord
from .db_utils import get_db_connection, execute_ddl # Use our new DB utilities
from typing import List, Dict, Any

# DDL statements (as defined in step 1 of the current plan)
# These should be executed once, e.g., when the application/pipeline starts.
PATIENTS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS patients (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    dob DATE,
    gender VARCHAR(50),
    address TEXT,
    email VARCHAR(255),
    phone VARCHAR(50),
    sex VARCHAR(50)
);
"""

DEVICE_READINGS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS device_readings (
    id VARCHAR(255) PRIMARY KEY,
    patient_id VARCHAR(255) REFERENCES patients(id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ NOT NULL,
    glucose NUMERIC(8, 2),
    systolic_bp INTEGER,
    diastolic_bp INTEGER,
    weight NUMERIC(8, 2)
);
"""

DEVICE_READINGS_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS idx_device_readings_patient_id_timestamp ON device_readings(patient_id, timestamp);
"""

ERROR_RECORDS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS error_records (
    error_id SERIAL PRIMARY KEY,
    reference TEXT,
    source_table VARCHAR(100),
    field_name VARCHAR(100),
    error_type VARCHAR(100),
    case_description TEXT,
    original_value TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
"""

ALL_DDL_STATEMENTS = [
    PATIENTS_TABLE_DDL,
    DEVICE_READINGS_TABLE_DDL,
    DEVICE_READINGS_INDEX_DDL,
    ERROR_RECORDS_TABLE_DDL
]

def initialize_database_schema():
    """
    Connects to the database and executes all DDL statements to create tables if they don't exist.
    """
    print("Attempting to initialize database schema...")
    conn = get_db_connection()
    if conn:
        try:
            if execute_ddl(conn, ALL_DDL_STATEMENTS):
                print("Database schema initialized (or already exists).")
            else:
                print("Failed to execute DDL statements for schema initialization.")
        finally:
            conn.close()
    else:
        print("Could not connect to database for schema initialization.")


def load_data(
    conn, # Expect a database connection to be passed in
    patients: List[Patient], 
    readings: List[DeviceReading]
) -> Dict[str, Any]:
    """
    Loads processed patient and device reading data into their respective PostgreSQL tables.
    Returns a summary of loaded data and any errors encountered during loading.
    """
    loaded_patients_count = 0
    loaded_readings_count = 0
    db_loading_errors = [] # For errors during this specific loading process

    if not conn:
        db_loading_errors.append({"type": "NO_DB_CONNECTION", "description": "No database connection provided to load_data."})
        return {
            "loaded_patients_count": 0, "loaded_readings_count": 0, 
            "db_loading_errors": db_loading_errors
        }

    try:
        with conn.cursor() as cur:
            # Load Patients
            for patient in patients:
                try:
                    cur.execute(
                        """
                        INSERT INTO patients (id, name, dob, gender, address, email, phone, sex)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING; 
                        """, 
                        (patient.id, patient.name, patient.dob, patient.gender, 
                         patient.address, patient.email, patient.phone, patient.sex)
                    )
                    if cur.rowcount > 0: # Check if a row was actually inserted
                        loaded_patients_count += 1
                except psycopg2.Error as e:
                    conn.rollback() # Rollback this specific patient insert
                    db_loading_errors.append({
                        "type": "PATIENT_INSERT_ERROR", "reference": patient.id, 
                        "description": str(e).split('\n')[0] # Get first line of error
                    })
                    # Re-open cursor if it was closed due to error (depends on error severity)
                    # For simplicity, we assume cursor is still usable or new one taken if this was a transaction block.
                    # Psycopg2 cursors usually remain open after most errors, just transaction is aborted.
                    # We are committing at the end, so individual rollbacks are for partial success.
                except Exception as e: # Catch any other unexpected error
                    conn.rollback()
                    db_loading_errors.append({
                        "type": "PATIENT_UNEXPECTED_ERROR", "reference": patient.id,
                        "description": str(e)
                    })


            # Load Device Readings
            for reading in readings:
                try:
                    # Ensure patient_id exists for the reading if it's a foreign key.
                    # The transformation step should ideally ensure this, or it's an optional FK.
                    # For now, we assume patient_id in the DeviceReading object is valid or None.
                    cur.execute(
                        """
                        INSERT INTO device_readings (id, patient_id, timestamp, glucose, systolic_bp, diastolic_bp, weight)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING;
                        """,
                        (reading.id, reading.patient_id, reading.timestamp, reading.glucose, 
                         reading.systolic_bp, reading.diastolic_bp, reading.weight)
                    )
                    if cur.rowcount > 0:
                        loaded_readings_count += 1
                except psycopg2.Error as e: # Specific psycopg2 errors (like IntegrityError for FK violation)
                    conn.rollback()
                    db_loading_errors.append({
                        "type": "READING_INSERT_ERROR", "reference": reading.id, 
                        "description": str(e).split('\n')[0]
                    })
                except Exception as e:
                     conn.rollback()
                     db_loading_errors.append({
                        "type": "READING_UNEXPECTED_ERROR", "reference": reading.id,
                        "description": str(e)
                    })
            
            conn.commit() # Commit all successful inserts for patients and readings

    except psycopg2.Error as e: # Error with cursor or connection during operations
        conn.rollback() # Rollback any uncommitted changes from the transaction
        db_loading_errors.append({"type": "DB_OPERATION_ERROR", "description": str(e)})
    except Exception as e: # Catch any other unexpected error
        conn.rollback()
        db_loading_errors.append({"type": "LOAD_DATA_UNEXPECTED_ERROR", "description": str(e)})


    return {
        "loaded_patients_count": loaded_patients_count,
        "loaded_readings_count": loaded_readings_count,
        "db_loading_errors": db_loading_errors
    }

def load_error_data(
    conn, # Expect a database connection
    errors: List[ErrorRecord]
) -> Dict[str, Any]:
    """
    Loads structured error records into the 'error_records' PostgreSQL table.
    """
    loaded_errors_count = 0
    db_error_loading_errors = []

    if not conn:
        db_error_loading_errors.append({"type": "NO_DB_CONNECTION", "description": "No database connection provided to load_error_data."})
        return {"loaded_errors_count": 0, "db_error_loading_errors": db_error_loading_errors}

    try:
        with conn.cursor() as cur:
            for error_record in errors:
                try:
                    cur.execute(
                        """
                        INSERT INTO error_records (reference, source_table, field_name, error_type, case_description, original_value)
                        VALUES (%s, %s, %s, %s, %s, %s); 
                        """, # created_at has a default
                        (str(error_record.reference), error_record.source_table, error_record.field_name, 
                         error_record.error_type, error_record.case_description, str(error_record.original_value))
                    )
                    # No ON CONFLICT for error_records, as each logged error should be unique via SERIAL PK.
                    loaded_errors_count += 1
                except psycopg2.Error as e:
                    conn.rollback()
                    db_error_loading_errors.append({
                        "type": "ERROR_RECORD_INSERT_ERROR", "reference": error_record.reference,
                        "description": str(e).split('\n')[0]
                    })
                except Exception as e:
                    conn.rollback()
                    db_error_loading_errors.append({
                        "type": "ERROR_RECORD_UNEXPECTED_ERROR", "reference": error_record.reference,
                        "description": str(e)
                    })
            conn.commit() # Commit all successful error inserts
            
    except psycopg2.Error as e:
        conn.rollback()
        db_error_loading_errors.append({"type": "DB_OPERATION_ERROR", "description": str(e)})
    except Exception as e:
        conn.rollback()
        db_error_loading_errors.append({"type": "LOAD_ERROR_DATA_UNEXPECTED_ERROR", "description": str(e)})

    return {
        "loaded_errors_count": loaded_errors_count,
        "db_error_loading_errors": db_error_loading_errors
    }

# The old in-memory storage and related functions (get_all_loaded_data, clear_storage) are removed.
# If this file is run standalone, it would need a way to get a DB connection.
if __name__ == '__main__':
    # This block is for example/testing; real execution is via main.py
    print("Testing loading.py with direct DB interaction (requires DB service via Docker Compose)")
    
    # 1. Initialize Schema (Idempotent)
    initialize_database_schema() # This internally gets and closes a connection

    # 2. Get a connection for loading
    conn_main = get_db_connection()
    if not conn_main:
        print("Failed to connect to DB for __main__ test in loading.py. Exiting.")
    else:
        try:
            # 3. Create Sample Data (using Pydantic models)
            # Make sure field names match Pydantic models AND DB columns
            sample_patients_data = [
                Patient(id="p1", name="John Doe", dob="1980-05-15", gender="Male", address="123 Test St", email="john.doe@example.com", phone="555-0101", sex="Male"),
                Patient(id="p2", name="Jane Smith", dob="1992-08-20", gender="Female", address="456 Sample Ave", email="jane.smith@example.com", phone="555-0202", sex="Female")
            ]
            # Patient p1's reading
            sample_readings_data = [
                DeviceReading(id="r1", patient_id="p1", timestamp="2023-04-01T10:30:00Z", glucose=105.5, systolic_bp=120, diastolic_bp=80, weight=150.7),
                DeviceReading(id="r2", patient_id="p1", timestamp="2023-04-02T11:00:00Z", glucose=99.0, systolic_bp=118, diastolic_bp=78, weight=150.0)
            ]
            # Error for patient p2 (example)
            sample_errors_data = [
                ErrorRecord(reference="p2", source_table="patients", field_name="email", error_type="DUPLICATE_EMAIL_HYPOTHETICAL", case_description="Email already exists (hypothetical example for error logging).", original_value="jane.smith@example.com")
            ]

            # 4. Load Data
            print("\n--- Loading Valid Data ---")
            load_summary = load_data(conn_main, sample_patients_data, sample_readings_data)
            print(f"Load Summary: {load_summary}")

            print("\n--- Loading Error Data ---")
            error_load_summary = load_error_data(conn_main, sample_errors_data)
            print(f"Error Load Summary: {error_load_summary}")

            # 5. Verify (Optional - crude verification, real verification via psql or DB tool)
            with conn_main.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM patients;")
                print(f"Patients count in DB: {cur.fetchone()[0]}")
                cur.execute("SELECT COUNT(*) FROM device_readings;")
                print(f"Device readings count in DB: {cur.fetchone()[0]}")
                cur.execute("SELECT COUNT(*) FROM error_records;")
                print(f"Error records count in DB: {cur.fetchone()[0]}")
        
        finally:
            conn_main.close()
            print("\n__main__ test in loading.py finished and connection closed.")
```
