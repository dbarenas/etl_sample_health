from pydantic import BaseModel, field_validator, Field, FieldValidationInfo
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import re

# --- Schema Definitions (Pydantic Models) ---
class Patient(BaseModel):
    name: str
    dob: str  # Will be validated and converted to datetime
    gender: str
    address: str
    email: str
    phone: str
    sex: str
    id: Optional[Any] = None # Added id as it was used in transform_patient reference

    @field_validator('dob')
    @classmethod
    def validate_dob(cls, value):
        try:
            datetime.strptime(value, '%Y-%m-%d')
        except ValueError:
            try:
                # Attempt to parse from MM/DD/YYYY and reformat
                dt_obj = datetime.strptime(value, '%m/%d/%Y')
                return dt_obj.strftime('%Y-%m-%d')
            except ValueError:
                raise ValueError("Invalid date format. Expected YYYY-MM-DD or MM/DD/YYYY.")
        return value

    @field_validator('email')
    @classmethod
    def validate_email(cls, value):
        if not re.match(r"[^@]+@[^@]+\.[^@]+", value):
            raise ValueError("Invalid email format.")
        return value

    @field_validator('phone')
    @classmethod
    def validate_phone(cls, value):
        # Basic phone validation: allows digits, hyphens, parentheses, spaces
        if not re.match(r"^[0-9\s\-\(\)]+$", value): # Simplified for example
            raise ValueError("Invalid phone format.")
        return value.strip()

    @field_validator('gender', 'sex')
    @classmethod
    def normalize_case(cls, value):
        return value.lower().capitalize()


class DeviceReading(BaseModel):
    patient_id: Optional[int] = None 
    timestamp: str 
    glucose: Optional[float] = None
    systolic_bp: Optional[int] = None
    diastolic_bp: Optional[int] = None
    weight: Optional[float] = None 
    reading_id: Optional[Any] = None # Added reading_id as it was used in transform_device_reading reference


    @field_validator('timestamp')
    @classmethod
    def validate_timestamp(cls, value):
        try:
            datetime.fromisoformat(value.replace('Z', '+00:00')) # Handles ISO format
        except ValueError:
            raise ValueError("Invalid timestamp format. Expected ISO format (e.g., YYYY-MM-DDTHH:MM:SSZ).")
        return value

    @field_validator('glucose')
    @classmethod
    def validate_glucose(cls, value):
        if value is not None and not (0 < value < 1000): # Plausible range
            raise ValueError("Glucose value out of plausible range (0-1000).")
        return value

    @field_validator('systolic_bp', 'diastolic_bp')
    @classmethod
    def validate_bp(cls, v: Optional[int], info: FieldValidationInfo):
        if v is not None and not (0 < v < 300): # Plausible range for BP
            raise ValueError(f"{info.field_name} value out of plausible range (0-300).")
        return v
    
    @field_validator('weight')
    @classmethod
    def validate_weight(cls, value):
        if value is not None and not (0 < value < 1000): # Plausible range for weight in lbs
            raise ValueError("Weight value out of plausible range (0-1000 lbs).")
        return value


class ErrorRecord(BaseModel):
    reference: Any 
    field_name: Optional[str] = None
    error_type: str 
    case_description: str
    original_value: Optional[Any] = None
    source_table: Optional[str] = None
