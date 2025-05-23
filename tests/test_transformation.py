import unittest
# Import models from etl.schemas now
from etl.schemas import Patient, DeviceReading, ErrorRecord
from etl.transformation import (
    transform_patient, transform_device_reading, pipeline_transform
)
from pydantic import ValidationError

class TestTransformation(unittest.TestCase):

    # --- Test Patient Model and transform_patient ---
    def test_transform_patient_valid(self):
        raw_data = {"id": 1, "name": "Valid Patient", "dob": "1990-12-01", "gender": "female", 
                    "address": "1 Test St", "email": "valid@example.com", "phone": "123-456-7890", "sex": "Female"}
        patient, error = transform_patient(raw_data, 0)
        self.assertIsNotNone(patient)
        self.assertIsNone(error)
        self.assertEqual(patient.name, "Valid Patient")
        self.assertEqual(patient.gender, "Female") # Normalized
        self.assertEqual(patient.sex, "Female") # Normalized
        self.assertEqual(patient.dob, "1990-12-01")

    def test_transform_patient_valid_alt_date_format(self):
        raw_data = {"id": 2, "name": "Alt Date", "dob": "03/15/1985", "gender": "Male", 
                    "address": "2 Test St", "email": "alt@example.com", "phone": "9876543210", "sex": "MALE"}
        patient, error = transform_patient(raw_data, 1)
        self.assertIsNotNone(patient)
        self.assertIsNone(error)
        self.assertEqual(patient.dob, "1985-03-15") # Validated and reformatted
        self.assertEqual(patient.gender, "Male")

    def test_transform_patient_invalid_dob(self):
        raw_data = {"id": 3, "name": "Invalid DOB", "dob": "1990-13-01", "gender": "Female", 
                    "address": "3 Test St", "email": "baddate@example.com", "phone": "1112223333", "sex": "Female"}
        patient, error = transform_patient(raw_data, 2)
        self.assertIsNone(patient)
        self.assertIsNotNone(error)
        self.assertEqual(error.reference, 3)
        self.assertEqual(error.field_name, "dob")
        self.assertIn("Invalid date format", error.case_description)
        self.assertEqual(error.original_value, "1990-13-01")

    def test_transform_patient_invalid_email(self):
        raw_data = {"id": 4, "name": "Invalid Email", "dob": "1990-01-01", "gender": "Female", 
                    "address": "4 Test St", "email": "invalid@", "phone": "2223334444", "sex": "Female"}
        patient, error = transform_patient(raw_data, 3)
        self.assertIsNone(patient)
        self.assertIsNotNone(error)
        self.assertEqual(error.field_name, "email")
        self.assertIn("Invalid email format", error.case_description)

    def test_transform_patient_missing_mandatory_field(self):
        # Pydantic models by default make fields mandatory unless Optional
        raw_data = {"id": 5, "dob": "1990-01-01", "gender": "Male", "address": "5 Test St", 
                    "email": "missing@example.com", "phone": "3334445555", "sex": "Male"} # Missing 'name'
        patient, error = transform_patient(raw_data, 4)
        self.assertIsNone(patient)
        self.assertIsNotNone(error)
        self.assertEqual(error.field_name, "name")
        self.assertIn("field required", error.case_description.lower()) # Pydantic v2 message
                                                                    # Pydantic v1: "none is not an allowed value" or similar if not passed

    # --- Test DeviceReading Model and transform_device_reading ---
    def test_transform_device_reading_valid(self):
        raw_data = {"reading_id": "d1", "timestamp": "2023-01-01T12:00:00Z", "glucose": 100.5, 
                    "systolic_bp": 120, "diastolic_bp": 80, "weight": 70.5}
        reading, error = transform_device_reading(raw_data, 0)
        self.assertIsNotNone(reading)
        self.assertIsNone(error)
        self.assertEqual(reading.glucose, 100.5)

    def test_transform_device_reading_invalid_timestamp(self):
        raw_data = {"reading_id": "d2", "timestamp": "2023/01/01 12:00:00", "glucose": 100.0}
        reading, error = transform_device_reading(raw_data, 1)
        self.assertIsNone(reading)
        self.assertIsNotNone(error)
        self.assertEqual(error.field_name, "timestamp")
        self.assertIn("Invalid timestamp format", error.case_description)

    def test_transform_device_reading_glucose_outlier_low(self):
        raw_data = {"reading_id": "d3", "timestamp": "2023-01-01T12:00:00Z", "glucose": -10.0}
        reading, error = transform_device_reading(raw_data, 2)
        # Pydantic validation error will be caught
        self.assertIsNone(reading)
        self.assertIsNotNone(error)
        self.assertEqual(error.field_name, "glucose")
        self.assertIn("Glucose value out of plausible range", error.case_description)
        self.assertEqual(error.error_type, "VALUE_ERROR") # Changed from VALIDATION_ERROR

    def test_transform_device_reading_glucose_outlier_high(self):
        raw_data = {"reading_id": "d4", "timestamp": "2023-01-01T12:00:00Z", "glucose": 2000.0}
        reading, error = transform_device_reading(raw_data, 3)
        self.assertIsNone(reading)
        self.assertIsNotNone(error)
        self.assertEqual(error.field_name, "glucose")
        self.assertIn("Glucose value out of plausible range", error.case_description)

    def test_transform_device_reading_bp_outlier(self):
        raw_data = {"reading_id": "d5", "timestamp": "2023-01-01T12:00:00Z", "systolic_bp": 350}
        reading, error = transform_device_reading(raw_data, 4)
        self.assertIsNone(reading)
        self.assertIsNotNone(error)
        self.assertEqual(error.field_name, "systolic_bp")
        self.assertIn("value out of plausible range", error.case_description)

    def test_transform_device_reading_invalid_type_string_for_number(self):
        raw_data = {"reading_id": "d6", "timestamp": "2023-01-01T12:00:00Z", "glucose": "not-a-number"}
        reading, error = transform_device_reading(raw_data, 5)
        self.assertIsNone(reading)
        self.assertIsNotNone(error)
        self.assertEqual(error.field_name, "glucose")
        # Pydantic error message for invalid float
        self.assertTrue("value is not a valid float" in error.case_description.lower() or \
                        "input should be a valid number" in error.case_description.lower()) 
        self.assertEqual(error.error_type, "INVALID_TYPE")

    def test_transform_device_reading_missing_value_handled_by_optional(self):
        # Glucose is Optional, so missing it should be fine
        raw_data = {"reading_id": "d7", "timestamp": "2023-01-01T12:00:00Z", "systolic_bp": 120}
        reading, error = transform_device_reading(raw_data, 6)
        self.assertIsNotNone(reading)
        self.assertIsNone(error)
        self.assertIsNone(reading.glucose)

    def test_transform_device_reading_logical_inconsistency_bp(self):
        raw_data = {"reading_id": "d8", "timestamp": "2023-01-01T12:00:00Z", 
                    "systolic_bp": 100, "diastolic_bp": 110} # Diastolic > Systolic
        reading, error = transform_device_reading(raw_data, 7)
        self.assertIsNone(reading) # The custom logic returns None for the record
        self.assertIsNotNone(error)
        self.assertEqual(error.field_name, "blood_pressure")
        self.assertEqual(error.error_type, "LOGICAL_INCONSISTENCY")
        self.assertIn("Diastolic BP is greater than or equal to Systolic BP", error.case_description)

    # --- Test pipeline_transform (Orchestrator) ---
    def test_pipeline_transform_empty_inputs(self):
        patients, readings, errors = pipeline_transform([], [])
        self.assertEqual(patients, [])
        self.assertEqual(readings, [])
        self.assertEqual(errors, [])

    def test_pipeline_transform_all_valid(self):
        raw_patients = [{"id": 1, "name": "P1", "dob": "1990-01-01", "gender": "F", "address": "A1", "email": "e1@e.com", "phone": "1", "sex": "F"}]
        raw_readings = [{"reading_id": "r1", "timestamp": "2023-01-01T10:00:00Z", "glucose": 100}]
        
        patients, readings, errors = pipeline_transform(raw_patients, raw_readings)
        self.assertEqual(len(patients), 1)
        self.assertEqual(len(readings), 1)
        self.assertEqual(len(errors), 0)
        self.assertEqual(patients[0].name, "P1")
        self.assertEqual(readings[0].reading_id, "r1")

    def test_pipeline_transform_all_invalid(self):
        raw_patients = [{"id": 1, "name": "Bad P", "dob": "bad-date"}] # Only partial, will fail validation
        raw_readings = [{"reading_id": "r1", "timestamp": "bad-time", "glucose": "NaN"}]
        
        patients, readings, errors = pipeline_transform(raw_patients, raw_readings)
        self.assertEqual(len(patients), 0)
        self.assertEqual(len(readings), 0)
        self.assertTrue(len(errors) >= 2) # At least one error for patient, one for reading
        
        patient_error_found = any(e.source_table == "patients" and e.reference == 1 for e in errors)
        reading_error_found = any(e.source_table == "device_readings" and e.reference == "r1" for e in errors)
        self.assertTrue(patient_error_found)
        self.assertTrue(reading_error_found)

    def test_pipeline_transform_mixed_valid_invalid(self):
        raw_patients = [
            {"id": 1, "name": "Good P", "dob": "1990-01-01", "gender": "M", "address": "A1", "email": "g@e.com", "phone": "1", "sex": "M"},
            {"id": 2, "name": "Bad P", "dob": "bad-date"}
        ]
        raw_readings = [
            {"reading_id": "r1", "timestamp": "2023-01-01T10:00:00Z", "glucose": 100},
            {"reading_id": "r2", "timestamp": "bad-time"}
        ]
        
        patients, readings, errors = pipeline_transform(raw_patients, raw_readings)
        self.assertEqual(len(patients), 1)
        self.assertEqual(patients[0].id, 1)
        self.assertEqual(len(readings), 1)
        self.assertEqual(readings[0].reading_id, "r1")
        self.assertEqual(len(errors), 2)

    def test_pipeline_transform_timestamp_order_inconsistency(self):
        raw_readings = [
            {"reading_id": "r1", "timestamp": "2023-01-01T10:00:00Z", "glucose": 100},
            {"reading_id": "r2", "timestamp": "2023-01-01T09:00:00Z", "glucose": 110}, # Earlier than r1
            {"reading_id": "r3", "timestamp": "2023-01-01T11:00:00Z", "glucose": 120}
        ]
        # Assuming patients data is empty for this specific test
        _, readings, errors = pipeline_transform([], raw_readings)
        
        self.assertEqual(len(readings), 3) # All readings are still processed
        
        timestamp_error_found = False
        for error in errors:
            if error.error_type == "TIMESTAMP_ORDER_INCONSISTENCY" and error.reference == "r2":
                timestamp_error_found = True
                break
        self.assertTrue(timestamp_error_found, "Timestamp order inconsistency error not found.")
        self.assertEqual(len(errors),1)


if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
