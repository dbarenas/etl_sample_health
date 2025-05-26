# api/crud.py
from sqlalchemy.orm import Session
from sqlalchemy import func, update, delete # For update and delete statements
from . import database as db_orm # Using db_orm to distinguish from pydantic models if needed
from . import models as pyd_models # Pydantic models for type hinting if needed for request data
from typing import List, Optional

# --- Patient CRUD ---
def get_patient(db: Session, patient_id: str) -> Optional[db_orm.Patient]:
    return db.query(db_orm.Patient).filter(db_orm.Patient.id == patient_id).first()

def get_patients(db: Session, skip: int = 0, limit: int = 100) -> List[db_orm.Patient]:
    return db.query(db_orm.Patient).offset(skip).limit(limit).all()

def count_patients(db: Session) -> int:
    return db.query(func.count(db_orm.Patient.id)).scalar()

# Note: Patient creation is assumed to be handled by ETL for now.
# If API were to create patients:
# def create_patient(db: Session, patient: pyd_models.PatientCreate) -> db_orm.Patient:
#     db_patient = db_orm.Patient(**patient.model_dump())
#     db.add(db_patient)
#     db.commit()
#     db.refresh(db_patient)
#     return db_patient


# --- DeviceReading CRUD ---
def get_device_reading(db: Session, device_reading_id: str) -> Optional[db_orm.DeviceReading]:
    return db.query(db_orm.DeviceReading).filter(db_orm.DeviceReading.id == device_reading_id).first()

def get_device_readings_for_patient(
    db: Session, 
    patient_id: str, 
    biometric_type: Optional[str] = None, 
    skip: int = 0, 
    limit: int = 100
) -> List[db_orm.DeviceReading]:
    query = db.query(db_orm.DeviceReading).filter(db_orm.DeviceReading.patient_id == patient_id)
    
    # Optional filtering by biometric type
    # This requires knowing which field corresponds to which biometric type.
    # Example: if biometric_type is 'glucose', filter where glucose is not null.
    if biometric_type:
        if biometric_type.lower() == 'glucose':
            query = query.filter(db_orm.DeviceReading.glucose.isnot(None))
        elif biometric_type.lower() == 'blood_pressure': # Could mean either systolic or diastolic present
            query = query.filter(
                (db_orm.DeviceReading.systolic_bp.isnot(None)) | 
                (db_orm.DeviceReading.diastolic_bp.isnot(None))
            )
        elif biometric_type.lower() == 'weight':
            query = query.filter(db_orm.DeviceReading.weight.isnot(None))
        # Add more specific filters as needed
        
    return query.order_by(db_orm.DeviceReading.timestamp.desc()).offset(skip).limit(limit).all()

def count_device_readings_for_patient(
    db: Session, 
    patient_id: str,
    biometric_type: Optional[str] = None
) -> int:
    query = db.query(func.count(db_orm.DeviceReading.id)).filter(db_orm.DeviceReading.patient_id == patient_id)
    if biometric_type:
        if biometric_type.lower() == 'glucose':
            query = query.filter(db_orm.DeviceReading.glucose.isnot(None))
        elif biometric_type.lower() == 'blood_pressure':
            query = query.filter(
                (db_orm.DeviceReading.systolic_bp.isnot(None)) | 
                (db_orm.DeviceReading.diastolic_bp.isnot(None))
            )
        elif biometric_type.lower() == 'weight':
            query = query.filter(db_orm.DeviceReading.weight.isnot(None))
    return query.scalar()

def upsert_device_reading(db: Session, reading_data: pyd_models.DeviceReadingCreate) -> db_orm.DeviceReading:
    """
    Upserts a device reading. 
    If a reading with the same 'id' exists, it's updated. Otherwise, a new one is created.
    This uses a direct approach; for true database-level upsert, ON CONFLICT DO UPDATE (PostgreSQL)
    could be used with raw SQL or SQLAlchemy core, but ORM approach is shown here.
    """
    # Convert Pydantic model to dictionary for ORM model creation/update
    reading_dict = reading_data.model_dump()
    
    db_reading = db.query(db_orm.DeviceReading).filter(db_orm.DeviceReading.id == reading_data.id).first()
    
    if db_reading:
        # Update existing record
        for key, value in reading_dict.items():
            setattr(db_reading, key, value)
    else:
        # Create new record
        # Ensure patient_id exists before creating a reading for them.
        # This check should ideally be here or enforced by FK constraints.
        patient = get_patient(db, reading_data.patient_id)
        if not patient:
            raise ValueError(f"Patient with id {reading_data.patient_id} not found. Cannot create device reading.")
            
        db_reading = db_orm.DeviceReading(**reading_dict)
        db.add(db_reading)
        
    db.commit()
    db.refresh(db_reading) # Refresh to get any DB-generated fields or confirm update
    return db_reading


def delete_device_reading(db: Session, device_reading_id: str) -> bool:
    """Deletes a device reading by its ID. Returns True if deleted, False otherwise."""
    db_reading = db.query(db_orm.DeviceReading).filter(db_orm.DeviceReading.id == device_reading_id).first()
    if db_reading:
        db.delete(db_reading)
        db.commit()
        return True
    return False

# --- Biometric Summary CRUD (Read-only from API perspective) ---
def get_patient_biometric_summary(db: Session, patient_id: str) -> Optional[db_orm.PatientBiometricSummary]:
    """
    Retrieves the biometric summary for a given patient_id.
    This data comes from the table generated by DBT.
    """
    return db.query(db_orm.PatientBiometricSummary).filter(db_orm.PatientBiometricSummary.patient_id == patient_id).first()

def get_all_biometric_summaries(db: Session, skip: int = 0, limit: int = 100) -> List[db_orm.PatientBiometricSummary]:
    """
    Retrieves all biometric summaries with pagination.
    """
    return db.query(db_orm.PatientBiometricSummary).offset(skip).limit(limit).all()

def count_all_biometric_summaries(db: Session) -> int:
    """
    Counts all biometric summaries.
    """
    return db.query(func.count(db_orm.PatientBiometricSummary.patient_id)).scalar()


if __name__ == '__main__':
    # Example of how to use these CRUD functions (requires a DB session)
    # This block won't run directly without setting up a session from database.py
    # and having the DB available.
    
    # from .database import SessionLocal, engine, Base
    # Base.metadata.create_all(bind=engine) # Ensure tables exist for a standalone test

    # db_session = SessionLocal()
    
    # Test get_patients
    # print("--- Testing get_patients ---")
    # initial_patients_count = count_patients(db_session)
    # print(f"Initial patient count: {initial_patients_count}")
    # patients = get_patients(db_session, limit=5)
    # for p in patients:
    #     print(f"Patient: {p.id}, Name: {p.name}")

    # Test device readings for a known patient (e.g., 'p1' if created by ETL)
    # print("
--- Testing device readings for patient 'p1' ---")
    # patient_p1_readings = get_device_readings_for_patient(db_session, patient_id='p1', limit=5)
    # for r in patient_p1_readings:
    #     print(f"Reading ID: {r.id}, Timestamp: {r.timestamp}, Glucose: {r.glucose}")
    
    # print("
--- Testing upsert_device_reading ---")
    # from .models import DeviceReadingCreate
    # from datetime import datetime
    # new_reading_data = DeviceReadingCreate(
    #     id="test_reading_001", # Unique ID for the new reading
    #     patient_id="p1",      # Assuming patient 'p1' exists
    #     timestamp=datetime.now(),
    #     glucose=123.45,
    #     weight=75.5
    # )
    # try:
    #     upserted_reading = upsert_device_reading(db_session, new_reading_data)
    #     print(f"Upserted reading: ID {upserted_reading.id}, Glucose {upserted_reading.glucose}")
        
    #     # Verify it's there
    #     fetched_reading = get_device_reading(db_session, "test_reading_001")
    #     assert fetched_reading is not None
    #     assert fetched_reading.glucose == 123.45
    #     print("Upsert verified by fetching.")

    #     # Test update part of upsert
    #     updated_reading_data = DeviceReadingCreate(
    #         id="test_reading_001",
    #         patient_id="p1",
    #         timestamp=datetime.now(), # Timestamp will change
    #         glucose=125.0, # Glucose value changed
    #         weight=76.0 # Weight changed
    #     )
    #     upserted_reading_updated = upsert_device_reading(db_session, updated_reading_data)
    #     print(f"Updated reading: ID {upserted_reading_updated.id}, Glucose {upserted_reading_updated.glucose}")
    #     assert upserted_reading_updated.glucose == 125.0
    #     print("Update part of upsert verified.")

    # except ValueError as ve:
    #     print(f"ValueError during upsert test: {ve}") # e.g. if patient 'p1' doesn't exist
    # except Exception as e:
    #     print(f"Error during upsert test: {e}")


    # print("
--- Testing delete_device_reading ---")
    # deleted = delete_device_reading(db_session, "test_reading_001")
    # print(f"Deletion status for 'test_reading_001': {deleted}")
    # assert deleted
    # deleted_reading_check = get_device_reading(db_session, "test_reading_001")
    # assert deleted_reading_check is None
    # print("Deletion verified.")


    # print("
--- Testing biometric summaries (assuming DBT has run) ---")
    # summaries = get_all_biometric_summaries(db_session, limit=5)
    # if summaries:
    #     for summary in summaries:
    #         print(f"Summary for Patient ID {summary.patient_id}: Name {summary.patient_name}, Avg Glucose: {summary.avg_glucose}")
    # else:
    #     print("No biometric summaries found. Ensure DBT models have been run.")

    # db_session.close()
```
