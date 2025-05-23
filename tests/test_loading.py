import unittest
from unittest.mock import patch, MagicMock, call # Import MagicMock and call
from etl.loading import (
    initialize_database_schema,
    load_data,
    load_error_data,
    ALL_DDL_STATEMENTS # To check DDL execution
)
# Import Pydantic models from schemas to create test data
from etl.schemas import Patient, DeviceReading, ErrorRecord
import psycopg2 # Import psycopg2 to mock its specific errors if necessary

class TestLoadingWithDBMock(unittest.TestCase):

    @patch('etl.loading.execute_ddl') # Mocks execute_ddl used by initialize_database_schema
    @patch('etl.loading.get_db_connection') # Mocks get_db_connection used by initialize_database_schema
    def test_initialize_database_schema_success(self, mock_get_db_conn, mock_execute_ddl):
        """Test schema initialization success path."""
        mock_conn = MagicMock()
        mock_get_db_conn.return_value = mock_conn
        mock_execute_ddl.return_value = True # Simulate DDL execution success

        initialize_database_schema()

        mock_get_db_conn.assert_called_once()
        mock_execute_ddl.assert_called_once_with(mock_conn, ALL_DDL_STATEMENTS)
        mock_conn.close.assert_called_once()
        # print("\nTest: initialize_database_schema_success PASSED")


    @patch('etl.loading.get_db_connection')
    def test_initialize_database_schema_no_connection(self, mock_get_db_conn):
        """Test schema initialization when DB connection fails."""
        mock_get_db_conn.return_value = None
        # We also need to ensure execute_ddl is not called if no connection
        with patch('etl.loading.execute_ddl') as mock_execute_ddl_local:
            initialize_database_schema()
            mock_execute_ddl_local.assert_not_called()
        # print("\nTest: test_initialize_database_schema_no_connection PASSED")


    @patch('etl.db_utils.get_db_connection') # Mock where it's called if needed, or pass mock conn directly
    def test_load_data_empty_lists(self, mock_get_db_connection_not_used_here):
        """Test load_data with empty patient and reading lists."""
        mock_conn = MagicMock() # Create a fresh mock connection for this test
        
        summary = load_data(mock_conn, [], [])
        
        self.assertEqual(summary["loaded_patients_count"], 0)
        self.assertEqual(summary["loaded_readings_count"], 0)
        self.assertEqual(len(summary["db_loading_errors"]), 0)
        # With current implementation, cursor is still obtained. Let's check execute is not called.
        # mock_conn.cursor.assert_called_once() # Cursor is obtained
        mock_conn.cursor.return_value.__enter__.return_value.execute.assert_not_called()
        # print("\nTest: load_data_empty_lists PASSED")

    def test_load_data_no_db_connection(self):
        """Test load_data when no DB connection is provided."""
        summary = load_data(None, [], [])
        self.assertEqual(summary["loaded_patients_count"], 0)
        self.assertEqual(summary["loaded_readings_count"], 0)
        self.assertTrue(len(summary["db_loading_errors"]) > 0)
        self.assertEqual(summary["db_loading_errors"][0]["type"], "NO_DB_CONNECTION")
        # print("\nTest: load_data_no_db_connection PASSED")


    def test_load_data_successful(self):
        """Test successful loading of patients and readings."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor # For 'with conn.cursor() as cur:'
        mock_cursor.rowcount = 1 # Simulate that each execute affects 1 row

        patients_to_load = [
            Patient(id="p1", name="P One", dob="2000-01-01", gender="F", address="Addr1", email="e1@example.com", phone="111", sex="F")
        ]
        readings_to_load = [
            DeviceReading(id="r1", patient_id="p1", timestamp="2023-01-01T00:00:00Z", glucose=100.0, systolic_bp=None, diastolic_bp=None, weight=None) # Explicitly None for optional
        ]

        summary = load_data(mock_conn, patients_to_load, readings_to_load)

        self.assertEqual(summary["loaded_patients_count"], 1)
        self.assertEqual(summary["loaded_readings_count"], 1)
        self.assertEqual(len(summary["db_loading_errors"]), 0)

        # Check patient insert
        mock_cursor.execute.assert_any_call(
            unittest.mock.ANY, # SQL string
            ("p1", "P One", "2000-01-01", "F", "Addr1", "e1@example.com", "111", "F")
        )
        # Check reading insert
        mock_cursor.execute.assert_any_call(
            unittest.mock.ANY, # SQL string
            ("r1", "p1", "2023-01-01T00:00:00Z", 100.0, None, None, None) 
        )
        mock_conn.commit.assert_called_once() # Should be called once after all operations
        # print("\nTest: load_data_successful PASSED")


    def test_load_data_patient_insert_db_error(self):
        """Test patient insert failure due to database error."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Simulate a DB error only for the first execute call (patient insert)
        # For the second call (reading insert), simulate success by not raising an error and setting rowcount.
        def execute_side_effect(sql, params):
            # Check based on the first element of params tuple which corresponds to 'id'
            if params[0] == "p_err": # Identifying the patient insert by its ID
                # print(f"Simulating DB error for patient: {params[0]}")
                raise psycopg2.Error("Simulated DB error for patient")
            elif params[0] == "r_ok": # Identifying the reading insert by its ID
                # print(f"Simulating success for reading: {params[0]}")
                mock_cursor.rowcount = 1 # Simulate successful insert for reading
                return None # No error
            return None # Default no error for other calls if any

        mock_cursor.execute.side_effect = execute_side_effect
        
        patients_to_load = [
            Patient(id="p_err", name="P Error", dob="2000-01-01", gender="M", address="AddrErr", email="err@example.com", phone="000", sex="M")
        ]
        readings_to_load = [ 
             DeviceReading(id="r_ok", patient_id="p_err", timestamp="2023-01-01T00:00:00Z", glucose=100.0, systolic_bp=None, diastolic_bp=None, weight=None)
        ]

        summary = load_data(mock_conn, patients_to_load, readings_to_load)
        
        self.assertEqual(summary["loaded_patients_count"], 0) # Patient failed
        self.assertEqual(summary["loaded_readings_count"], 1) # Reading succeeded
        
        self.assertEqual(len(summary["db_loading_errors"]), 1)
        self.assertEqual(summary["db_loading_errors"][0]["type"], "PATIENT_INSERT_ERROR")
        self.assertEqual(summary["db_loading_errors"][0]["reference"], "p_err")
        
        mock_conn.rollback.assert_any_call() 
        mock_conn.commit.assert_called_once() 
        # print("\nTest: load_data_patient_insert_db_error PASSED")

    # --- Tests for load_error_data ---
    def test_load_error_data_successful(self):
        """Test successful loading of error records."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.rowcount = 1 # Simulate successful insert

        errors_to_load = [
            ErrorRecord(reference="ref1", source_table="patients", field_name="email", 
                        error_type="INVALID_FORMAT", case_description="Bad email", original_value="abc")
        ]
        summary = load_error_data(mock_conn, errors_to_load)

        self.assertEqual(summary["loaded_errors_count"], 1)
        self.assertEqual(len(summary["db_error_loading_errors"]), 0)
        mock_cursor.execute.assert_called_once_with(
            unittest.mock.ANY, # SQL string
            ("ref1", "patients", "email", "INVALID_FORMAT", "Bad email", "abc")
        )
        mock_conn.commit.assert_called_once()
        # print("\nTest: load_error_data_successful PASSED")

    def test_load_error_data_db_error(self):
        """Test error record insert failure."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.execute.side_effect = psycopg2.Error("Simulated DB error for error_record")
        mock_cursor.rowcount = 0 # Simulate no row affected

        errors_to_load = [
            ErrorRecord(reference="ref_err", source_table="readings", field_name="glucose", 
                        error_type="OUTLIER", case_description="Too high", original_value="10000")
        ]
        summary = load_error_data(mock_conn, errors_to_load)

        self.assertEqual(summary["loaded_errors_count"], 0)
        self.assertEqual(len(summary["db_error_loading_errors"]), 1)
        self.assertEqual(summary["db_error_loading_errors"][0]["type"], "ERROR_RECORD_INSERT_ERROR")
        mock_conn.rollback.assert_called_once() 
        mock_conn.commit.assert_called_once() # commit() is called after the loop regardless of errors inside in current implementation
        # print("\nTest: load_error_data_db_error PASSED")

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
```
