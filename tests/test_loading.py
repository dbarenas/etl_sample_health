import unittest
from etl.loading import load_data, load_error_data, get_all_loaded_data, clear_storage
# Models are now imported from etl.schemas
try:
    from etl.schemas import Patient, DeviceReading, ErrorRecord 
except ImportError:
    # Fallback simplified model definitions for standalone testing
    # These should ideally match the structure of those in schemas.py if used.
    from pydantic import BaseModel, Field as PydanticField
    from typing import Any, Optional as PyOptional # Renamed to avoid conflict
    class Patient(BaseModel): # type: ignore
        id: PyOptional[Any] = None # Match schema
        name: str
        # Add other fields from schema if necessary for specific tests using this fallback
        dob: str = "1900-01-01" 
        gender: str = "Unknown"
        address: str = "N/A"
        email: str = "test@example.com"
        phone: str = "N/A"
        sex: str = "Unknown"

    class DeviceReading(BaseModel): # type: ignore
        reading_id: PyOptional[Any] = None # Match schema
        timestamp: str = "1900-01-01T00:00:00Z"
        # Add other fields like glucose, patient_id etc. if needed for fallback tests
        patient_id: PyOptional[int] = None
        glucose: PyOptional[float] = None


    class ErrorRecord(BaseModel): # type: ignore
        reference: Any
        error_type: str
        case_description: str # Renamed from description to match schema
        field_name: PyOptional[str] = None
        original_value: PyOptional[Any] = None
        source_table: PyOptional[str] = None


class TestLoading(unittest.TestCase):

    def setUp(self):
        """Clear storage before each test to ensure independence."""
        clear_storage()

    def tearDown(self):
        """Clear storage after each test to clean up."""
        clear_storage()

    def test_load_data_empty(self):
        summary = load_data([], [])
        self.assertEqual(summary["loaded_patients_count"], 0)
        self.assertEqual(summary["loaded_readings_count"], 0)
        self.assertEqual(summary["total_patients_in_storage"], 0)
        self.assertEqual(summary["total_readings_in_storage"], 0)
        self.assertEqual(len(summary["loading_errors"]), 0)

        all_data = get_all_loaded_data()
        self.assertEqual(len(all_data["patients"]), 0)
        self.assertEqual(len(all_data["device_readings"]), 0)

    def test_load_error_data_empty(self):
        summary = load_error_data([])
        self.assertEqual(summary["loaded_errors_count"], 0)
        self.assertEqual(summary["total_errors_in_storage"], 0)
        
        all_data = get_all_loaded_data()
        self.assertEqual(len(all_data["errors"]), 0)

    def test_load_data_successful(self):
        # Use the imported (or fallback) Patient and DeviceReading models
        patients_to_load = [Patient(id=1, name="P1", dob="1990-01-01", gender="F", address="A1", email="e@e.com", phone="1", sex="F")]
        readings_to_load = [DeviceReading(reading_id="r1", patient_id=1, timestamp="2023-01-01T00:00:00Z", glucose=100.0)]

        summary = load_data(patients_to_load, readings_to_load) 
        self.assertEqual(summary["loaded_patients_count"], 1)
        self.assertEqual(summary["loaded_readings_count"], 1)
        self.assertEqual(summary["total_patients_in_storage"], 1)
        self.assertEqual(summary["total_readings_in_storage"], 1)

        all_data = get_all_loaded_data()
        self.assertEqual(len(all_data["patients"]), 1)
        self.assertEqual(all_data["patients"][0], patients_to_load[0]) 
        self.assertEqual(len(all_data["device_readings"]), 1)
        self.assertEqual(all_data["device_readings"][0], readings_to_load[0])


    def test_load_error_data_successful(self):
        errors_to_load = [ErrorRecord(reference="ref1", error_type="TEST_ERROR", case_description="A test error")]
        
        summary = load_error_data(errors_to_load) 
        self.assertEqual(summary["loaded_errors_count"], 1)
        self.assertEqual(summary["total_errors_in_storage"], 1)

        all_data = get_all_loaded_data()
        self.assertEqual(len(all_data["errors"]), 1)
        self.assertEqual(all_data["errors"][0], errors_to_load[0])


    def test_load_data_multiple_calls_cumulative(self):
        patients1 = [Patient(id=1, name="P1", dob="1990-01-01", gender="F", address="A1", email="e@e.com", phone="1", sex="F")]
        patients2 = [Patient(id=2, name="P2", dob="1990-01-01", gender="M", address="A2", email="e2@e.com", phone="2", sex="M")]

        load_data(patients1, []) 
        summary = load_data(patients2, []) 

        self.assertEqual(summary["loaded_patients_count"], 1) 
        self.assertEqual(summary["total_patients_in_storage"], 2) 

        all_data = get_all_loaded_data()
        self.assertEqual(len(all_data["patients"]), 2)

    def test_clear_storage_works(self):
        patients = [Patient(id=1, name="P1", dob="1990-01-01", gender="F", address="A1", email="e@e.com", phone="1", sex="F")]
        errors = [ErrorRecord(reference="ref1", error_type="E1", case_description="D1")]
            
        load_data(patients, []) 
        load_error_data(errors) 

        all_data_before_clear = get_all_loaded_data()
        self.assertEqual(len(all_data_before_clear["patients"]), 1)
        self.assertEqual(len(all_data_before_clear["errors"]), 1)

        clear_storage()
        all_data_after_clear = get_all_loaded_data()
        self.assertEqual(len(all_data_after_clear["patients"]), 0)
        self.assertEqual(len(all_data_after_clear["device_readings"]), 0)
        self.assertEqual(len(all_data_after_clear["errors"]), 0)


if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
