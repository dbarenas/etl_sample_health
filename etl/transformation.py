from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from pydantic import ValidationError # BaseModel, validator are now in schemas
from .schemas import Patient, DeviceReading, ErrorRecord # Import models from schemas.py
import re


# --- Transformation Functions ---

def transform_patient(patient_record: Dict[str, Any], record_index: int) -> Tuple[Optional[Patient], Optional[ErrorRecord]]:
    """Transforms a single patient record."""
    try:
        # 'id' is now part of the Patient schema, Pydantic will handle it if present in patient_record
        # If 'id' is not in patient_record, it will be None in the model (if Optional)
        # or cause validation error (if not Optional and no default).
        # The reference for ErrorRecord should still use .get('id', record_index) for robustness
        # against raw data that might or might not have 'id'.
        patient_ref_id = patient_record.get('id', record_index)
        
        # Field mapping (if raw keys differ from model keys) could be done here
        # For now, assume direct mapping

        validated_patient = Patient(**patient_record)
        # If 'id' was missing in raw_data but is needed for the object, ensure it's set if possible.
        # However, Pydantic model handles 'id' based on its definition (Optional or required).
        # If it's Optional and not provided, validated_patient.id will be None.
        # If we want to ensure the 'id' field in the Patient object is populated with record_index
        # when not present in the raw data, we'd do it here, AFTER initial validation:
        if validated_patient.id is None:
            validated_patient.id = record_index # Or patient_ref_id

        return validated_patient, None
    except ValidationError as e:
        errors = e.errors()
        first_error = errors[0]
        field = first_error['loc'][0] if first_error['loc'] else 'general'
        error_rec = ErrorRecord(
            reference=patient_ref_id, # Use the determined reference ID
            field_name=str(field),
            error_type="INVALID_FORMAT" if "format" in first_error['msg'].lower() else "VALIDATION_ERROR",
            case_description=first_error['msg'],
            original_value=patient_record.get(str(field)),
            source_table="patients"
        )
        return None, error_rec
    except Exception as e: # Catch any other unexpected error during transformation
        patient_ref_id = patient_record.get('id', record_index)
        error_rec = ErrorRecord(
            reference=patient_ref_id, # Use the determined reference ID
            error_type="TRANSFORMATION_ERROR",
            case_description=str(e),
            original_value=str(patient_record), # Keep as string for unforeseen errors
            source_table="patients"
        )
        return None, error_rec


def transform_device_reading(reading_record: Dict[str, Any], record_index: int) -> Tuple[Optional[DeviceReading], Optional[ErrorRecord]]:
    """Transforms a single device reading record."""
    try:
        # Similar to patient, use .get() for reference in ErrorRecord
        reading_ref_id = reading_record.get('reading_id', record_index)

        # Handle potential string to number conversions for relevant fields
        # Pydantic will try to coerce, but explicit is safer for some CSV inputs
        for field in ['glucose', 'systolic_bp', 'diastolic_bp', 'weight']:
            if field in reading_record and isinstance(reading_record[field], str):
                if reading_record[field] == '': # Handle empty strings as None
                    reading_record[field] = None
                else:
                    try:
                        # Attempt conversion, but let Pydantic handle errors primarily
                        if '.' in reading_record[field]: 
                            reading_record[field] = float(reading_record[field])
                        else: 
                            reading_record[field] = int(reading_record[field])
                    except ValueError:
                        # Pydantic will catch this if it's still not a valid type.
                        pass 
        
        validated_reading = DeviceReading(**reading_record)
        # Ensure reading_id is populated in the object if it was missing from raw data
        if validated_reading.reading_id is None:
            validated_reading.reading_id = reading_ref_id


        # Additional checks not covered by Pydantic field validators
        if validated_reading.systolic_bp is not None and validated_reading.diastolic_bp is not None:
            if validated_reading.diastolic_bp >= validated_reading.systolic_bp:
                error_rec = ErrorRecord(
                    reference=reading_ref_id, # Use determined reference
                    field_name="blood_pressure",
                    error_type="LOGICAL_INCONSISTENCY",
                    case_description="Diastolic BP is greater than or equal to Systolic BP.",
                    original_value=f"Systolic: {validated_reading.systolic_bp}, Diastolic: {validated_reading.diastolic_bp}",
                    source_table="device_readings"
                )
                return None, error_rec

        return validated_reading, None
    except ValidationError as e:
        errors = e.errors()
        first_error = errors[0]
        field = first_error['loc'][0] if first_error['loc'] else 'general'
        error_type = "INVALID_FORMAT" # Default
        msg_lower = first_error['msg'].lower()
        err_type_from_pydantic = first_error.get('type') # Pydantic v2 error type e.g. 'value_error', 'missing', 'float_parsing'

        # Prioritize custom messages if specific keywords are present
        if "out of plausible range" in msg_lower: # This is from our custom validators
            error_type = "VALUE_ERROR"
        elif "invalid date format" in msg_lower or \
             "invalid timestamp format" in msg_lower or \
             "invalid email format" in msg_lower or \
             "invalid phone format" in msg_lower : # These are also from our custom validators
            error_type = "INVALID_FORMAT" 
        # Then check Pydantic's own error types for more generic cases
        elif err_type_from_pydantic == 'missing':
            error_type = "MISSING_VALUE"
        elif err_type_from_pydantic in ['int_parsing', 'float_parsing', 'string_type', 'bool_parsing', 'datetime_parsing', 'finite_number']:
            error_type = "INVALID_TYPE"
        # 'value_error' from Pydantic can be broad. Our custom "out of plausible range" is more specific.
        # If it's a Pydantic 'value_error' not caught by our custom messages, then it's a general VALUE_ERROR.
        elif err_type_from_pydantic in ['value_error', 'assertion_error', 'less_than', 'greater_than', 'less_than_equal', 'greater_than_equal', 'multiple_of']:
             error_type = "VALUE_ERROR" 
        # Fallback checks based on message content if Pydantic type wasn't specific or caught above
        elif "value is not a valid float" in msg_lower or \
             "value is not a valid integer" in msg_lower or \
             "input should be a valid number" in msg_lower or \
             "input should be a valid integer" in msg_lower:
            error_type = "INVALID_TYPE"
        elif "field required" in msg_lower or \
             "missing" in msg_lower : # Catches cases where Pydantic type might be 'missing_key' etc.
            error_type = "MISSING_VALUE"
        
        error_rec = ErrorRecord(
            reference=reading_ref_id, # Use determined reference
            field_name=str(field),
            error_type=error_type,
            case_description=first_error['msg'],
            original_value=reading_record.get(str(field)),
            source_table="device_readings"
        )
        return None, error_rec
    except Exception as e:
        reading_ref_id = reading_record.get('reading_id', record_index)
        error_rec = ErrorRecord(
            reference=reading_ref_id, # Use determined reference
            error_type="TRANSFORMATION_ERROR",
            case_description=str(e),
            original_value=str(reading_record), # Keep as string
            source_table="device_readings"
        )
        return None, error_rec

def pipeline_transform(
    raw_patient_data: List[Dict[str, Any]],
    raw_device_data: List[Dict[str, Any]]
) -> Tuple[List[Patient], List[DeviceReading], List[ErrorRecord]]:
    """
    Orchestrates the transformation of all extracted patient and device data.
    """
    processed_patients: List[Patient] = []
    processed_readings: List[DeviceReading] = []
    all_error_records: List[ErrorRecord] = []

    # Transform Patient Data
    for i, record in enumerate(raw_patient_data):
        patient, error = transform_patient(record, i)
        if patient:
            processed_patients.append(patient)
        if error:
            all_error_records.append(error)

    # Transform Device Reading Data
    last_timestamp_dict: Dict[Any, datetime] = {} # Store last timestamp per patient_id if available

    for i, record in enumerate(raw_device_data):
        reading, error = transform_device_reading(record, i)
        
        current_error_list = []
        if error:
            current_error_list.append(error)

        if reading:
            # Timestamp order validation (potentially per patient if patient_id is available)
            current_timestamp_str = reading.timestamp
            patient_key_for_ts_check = reading.patient_id if reading.patient_id is not None else "global"

            try:
                current_timestamp = datetime.fromisoformat(current_timestamp_str.replace('Z', '+00:00'))
                
                last_timestamp = last_timestamp_dict.get(patient_key_for_ts_check)

                if last_timestamp and current_timestamp < last_timestamp:
                    ts_error = ErrorRecord(
                        reference=reading.reading_id if reading.reading_id is not None else record.get('reading_id',i), # Use model's reading_id
                        field_name="timestamp",
                        error_type="TIMESTAMP_ORDER_INCONSISTENCY",
                        case_description=f"Timestamp {current_timestamp_str} is earlier than previous {last_timestamp.isoformat()} for key {patient_key_for_ts_check}",
                        original_value=current_timestamp_str,
                        source_table="device_readings"
                    )
                    current_error_list.append(ts_error)
                    # This record might still be added to processed_readings, error is just logged.
                
                # Update last_timestamp only if current is not older (or if it's the first one)
                if not last_timestamp or current_timestamp >= last_timestamp:
                     last_timestamp_dict[patient_key_for_ts_check] = current_timestamp

            except ValueError:
                # This error should ideally be caught during Pydantic validation of the timestamp.
                # If it occurs here, it means Pydantic validation might have passed (e.g. if format was okay but value was nonsense)
                # or the error from Pydantic wasn't added to current_error_list yet.
                # For robustness, can add a fallback error, but usually Pydantic handles this.
                pass 

            processed_readings.append(reading)
        
        for e in current_error_list:
             if e not in all_error_records: # Avoid duplicates if error was already added
                all_error_records.append(e)


    return processed_patients, processed_readings, all_error_records

if __name__ == '__main__':
    # Example Usage
    # Ensure that sample data now includes 'id' for patients and 'reading_id' for devices if they are expected by models
    # or handled by the get('id', index) logic for error reporting.
    # The schemas.py models now have Optional id and reading_id.
    sample_patients_raw = [
        {"id": 1, "name": "Alice Wonderland", "dob": "1990-01-01", "gender": "Female", "address": "123 Main St", "email": "alice@example.com", "phone": "555-1234", "sex": "Female"},
        {"id": 2, "name": "Bob The Builder", "dob": "03/15/1985", "gender": "Male", "address": "456 Side St", "email": "bob@", "phone": "555-5678", "sex": "Male"}, # invalid email
        {"name": "No ID Charlie", "dob": "1950-07-30", "gender": "MALE", "address": "789 Other St", "email": "charlie@goodgrief.com", "phone": "valid-phone1", "sex": "male"}, # No 'id'
        {"id": "p4", "name": "Invalid Date", "dob": "1990/01/01", "gender": "Female", "address": "Error Lane", "email": "error@example.com", "phone": "555-0000", "sex": "Female"}, # invalid dob
    ]

    sample_readings_raw = [
        {"reading_id": "r1", "patient_id": 1, "timestamp": "2023-01-01T10:00:00Z", "glucose": 120.5, "systolic_bp": 120, "diastolic_bp": 80, "weight": 150.0},
        {"reading_id": "r2", "patient_id": 1, "timestamp": "2023-01-01T09:00:00Z", "glucose": 110.0, "systolic_bp": 118, "diastolic_bp": 78, "weight": 150.5}, # Timestamp order issue for patient 1
        {"patient_id": 2, "timestamp": "2023-01-02T12:00:00Z", "glucose": "high", "systolic_bp": 140, "diastolic_bp": 90, "weight": 200.0}, # No reading_id, Invalid glucose type
        {"reading_id": "r4", "patient_id": 2, "timestamp": "2023-01-02T14:00:00Z", "glucose": 99.0, "systolic_bp": 130, "diastolic_bp": 150, "weight": 198.0}, # Diastolic > Systolic
        {"reading_id": "r5", "patient_id": "p4", "timestamp": "invalid_timestamp", "glucose": 100.0, "systolic_bp": 120, "diastolic_bp": 80, "weight": 160.0}, # invalid timestamp
        {"reading_id": "r6", "patient_id": 1, "timestamp": "2023-01-01T11:00:00Z", "glucose": 5000, "systolic_bp": 125, "diastolic_bp": 75, "weight": "unknown"}, # Glucose outlier, weight type
    ]

    print("--- Transforming Data ---")
    valid_patients, valid_readings, errors = pipeline_transform(sample_patients_raw, sample_readings_raw)

    print(f"\n--- Valid Patients ({len(valid_patients)}) ---")
    for p in valid_patients:
        print(p.model_dump_json(indent=2))

    print(f"\n--- Valid Device Readings ({len(valid_readings)}) ---")
    for r in valid_readings:
        print(r.model_dump_json(indent=2))

    print(f"\n--- Error Records ({len(errors)}) ---")
    for err in errors:
        print(err.model_dump_json(indent=2))
