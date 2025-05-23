import unittest
import os
import json
import csv
from etl.extraction import extract_json, extract_csv, extract_data

class TestExtraction(unittest.TestCase):

    def setUp(self):
        """Set up test files before each test."""
        self.test_data_dir = "test_data_temp"
        os.makedirs(self.test_data_dir, exist_ok=True)

        self.valid_json_path = os.path.join(self.test_data_dir, "patients_valid.json")
        self.malformed_json_path = os.path.join(self.test_data_dir, "patients_malformed.json")
        self.empty_json_path = os.path.join(self.test_data_dir, "patients_empty.json")

        self.valid_csv_path = os.path.join(self.test_data_dir, "devices_valid.csv")
        self.malformed_csv_path = os.path.join(self.test_data_dir, "devices_malformed.csv") # Difficult to truly malform DictReader easily
        self.empty_csv_path = os.path.join(self.test_data_dir, "devices_empty.csv")

        # Create valid JSON
        with open(self.valid_json_path, 'w') as f:
            json.dump([{"id": 1, "name": "Test Patient"}], f)
        
        # Create malformed JSON (missing comma)
        with open(self.malformed_json_path, 'w') as f:
            f.write('[{"id": 1, "name": "Test Patient"} {"id": 2, "name": "Another"}]') # Invalid JSON

        # Create empty JSON file (technically invalid for json.load if it's truly empty)
        with open(self.empty_json_path, 'w') as f:
            f.write("") # Empty file

        # Create valid CSV
        with open(self.valid_csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["device_id", "value"])
            writer.writerow(["dev1", "100"])
        
        # Create empty CSV (only headers)
        with open(self.empty_csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["device_id", "value"])


    def tearDown(self):
        """Clean up test files after each test."""
        for root, dirs, files in os.walk(self.test_data_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(self.test_data_dir)

    # --- Test extract_json ---
    def test_extract_json_success(self):
        data = extract_json(self.valid_json_path)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["name"], "Test Patient")

    def test_extract_json_file_not_found(self):
        # Suppress print output during this test
        import sys
        from io import StringIO
        original_stdout = sys.stdout
        sys.stdout = StringIO()
        
        data = extract_json("non_existent.json")
        self.assertEqual(data, [])
        
        sys.stdout = original_stdout # Restore stdout

    def test_extract_json_malformed(self):
        import sys
        from io import StringIO
        original_stdout = sys.stdout
        sys.stdout = StringIO()

        data = extract_json(self.malformed_json_path)
        self.assertEqual(data, [])

        sys.stdout = original_stdout

    def test_extract_json_empty_file(self):
        # An empty file is not valid JSON for json.load()
        import sys
        from io import StringIO
        original_stdout = sys.stdout
        sys.stdout = StringIO()
        
        data = extract_json(self.empty_json_path)
        self.assertEqual(data, [])
        
        sys.stdout = original_stdout


    # --- Test extract_csv ---
    def test_extract_csv_success(self):
        data = extract_csv(self.valid_csv_path)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["device_id"], "dev1")
        self.assertEqual(data[0]["value"], "100")

    def test_extract_csv_file_not_found(self):
        import sys
        from io import StringIO
        original_stdout = sys.stdout
        sys.stdout = StringIO()

        data = extract_csv("non_existent.csv")
        self.assertEqual(data, [])

        sys.stdout = original_stdout

    def test_extract_csv_empty_file(self):
        # CSV with only headers
        data = extract_csv(self.empty_csv_path)
        self.assertEqual(data, [])
    
    # Note: Testing malformed CSVs where DictReader itself fails (e.g. completely unparsable)
    # is tricky as DictReader can be quite robust or fail in ways that might not just return [].
    # The IOError in extract_csv is a general catch-all.

    # --- Test extract_data (orchestrator) ---
    def test_extract_data_success_defaults(self):
        patients, devices = extract_data(self.valid_json_path, self.valid_csv_path)
        self.assertEqual(len(patients), 1)
        self.assertEqual(patients[0]["name"], "Test Patient")
        self.assertEqual(len(devices), 1)
        self.assertEqual(devices[0]["device_id"], "dev1")

    def test_extract_data_custom_types(self):
        # Create a dummy CSV for patients and JSON for devices for this test
        patient_csv_path = os.path.join(self.test_data_dir, "patients_temp.csv")
        device_json_path = os.path.join(self.test_data_dir, "devices_temp.json")

        with open(patient_csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name"])
            writer.writerow(["p_csv", "Patient CSV"])
        
        with open(device_json_path, 'w') as f:
            json.dump([{"device_id": "d_json", "value": "200"}], f)

        patients, devices = extract_data(patient_csv_path, device_json_path, 
                                         patient_file_type='csv', device_file_type='json')
        self.assertEqual(len(patients), 1)
        self.assertEqual(patients[0]["name"], "Patient CSV")
        self.assertEqual(len(devices), 1)
        self.assertEqual(devices[0]["device_id"], "d_json")

    def test_extract_data_one_file_fails(self):
        import sys
        from io import StringIO
        original_stdout = sys.stdout
        sys.stdout = StringIO()

        # Valid JSON, non-existent CSV
        patients, devices = extract_data(self.valid_json_path, "non_existent.csv")
        self.assertEqual(len(patients), 1)
        self.assertEqual(patients[0]["name"], "Test Patient")
        self.assertEqual(len(devices), 0) # Devices should be empty

        # Non-existent JSON, valid CSV
        patients, devices = extract_data("non_existent.json", self.valid_csv_path)
        self.assertEqual(len(patients), 0) # Patients should be empty
        self.assertEqual(len(devices), 1)
        self.assertEqual(devices[0]["device_id"], "dev1")
        
        sys.stdout = original_stdout

    def test_extract_data_unsupported_file_types(self):
        import sys
        from io import StringIO
        original_stdout = sys.stdout
        sys.stdout = StringIO()

        patients, devices = extract_data(self.valid_json_path, self.valid_csv_path, 
                                         patient_file_type='xml', device_file_type='txt')
        self.assertEqual(len(patients), 0)
        self.assertEqual(len(devices), 0)
        
        output = sys.stdout.getvalue()
        self.assertIn("Unsupported file type for patient data: xml", output)
        self.assertIn("Unsupported file type for device data: txt", output)

        sys.stdout = original_stdout

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False) # exit=False to run in some environments like Jupyter
