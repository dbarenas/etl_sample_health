# ETL Pipeline for Patient and Device Data

## 1. Overview

This project implements an ETL (Extract, Transform, Load) pipeline designed to process patient demographic data and medical device readings. It extracts data from JSON and CSV files, transforms it according to defined schemas and validation rules, and then loads the processed data and any identified errors into a PostgreSQL database. The pipeline is built with Python 3.11+, uses Pydantic for data validation, and can be containerized using Docker. A dbt (Data Build Tool) project is included for transforming data within the data warehouse. Finally, a FastAPI application provides a RESTful API to interact with the processed data.

## 2. Features Implemented

*   **Extraction**:
    *   Reads patient data from JSON files.
    *   Reads device reading data from CSV files.
    *   Handles file not found and malformed file errors.
    *   Unified `extract_data` function to orchestrate data ingestion.
*   **Transformation (Python ETL)**:
    *   **Schema Validation**: Uses Pydantic models (`etl/schemas.py`) to define and enforce schemas for patient and device data.
        *   `Patient` schema: `id`, `name`, `dob`, `gender`, `address`, `email`, `phone`, `sex`.
        *   `DeviceReading` schema: `id` (formerly `reading_id`), `patient_id`, `timestamp`, `glucose`, `systolic_bp`, `diastolic_bp`, `weight`.
    *   **Data Normalization**:
        *   Converts date of birth (DOB) to `YYYY-MM-DD` format (accepts `MM/DD/YYYY` as input).
        *   Normalizes `gender` and `sex` fields to title case (e.g., "Female").
    *   **Validation Rules**:
        *   Validates email and phone number formats (basic regex).
        *   Ensures numeric fields (glucose, BP, weight) are within plausible ranges.
        *   Checks timestamp format (ISO 8601).
    *   **Error Handling**:
        *   Identifies records failing validation (missing fields, invalid formats, outliers, etc.).
        *   Logs logical inconsistencies (e.g., diastolic BP > systolic BP).
        *   Checks for timestamp order for device readings (per patient if `patient_id` is available, otherwise globally).
        *   Generates structured `ErrorRecord` objects for each issue, detailing the problem.
*   **Loading (Python ETL)**:
    *   Loads successfully transformed `Patient` and `DeviceReading` objects into respective PostgreSQL tables.
    *   Loads `ErrorRecord` objects into a PostgreSQL table for errors identified during transformation.
    *   Handles potential database errors during loading (e.g., duplicate primary keys via `ON CONFLICT DO NOTHING`).
    *   Initializes database schema (creates tables if they don't exist) upon pipeline startup.
*   **Data Transformation & Analytics (dbt)**:
    *   **Source Definition**: Defines raw tables loaded by Python ETL as dbt sources.
    *   **Staging Models**: Cleans and prepares source data (e.g., `stg_patients`, `stg_device_readings`). Materialized as views.
    *   **Mart Models**: Creates analytical models, such as `patient_biometric_summary`, which calculates MIN/MAX/AVG for key biometrics per patient. Materialized as tables.
*   **FastAPI Application (API)**:
    *   Provides RESTful endpoints to interact with patient data, device readings, and biometric summaries.
    *   Uses SQLAlchemy ORM for database interaction and Pydantic for request/response validation.
    *   Includes CRUD-like operations for device readings (upsert, delete).
    *   Offers paginated responses for lists of resources.
*   **Asynchronous Pipeline (Python ETL)**:
    *   The main pipeline (`main.py`) orchestrates extraction, transformation, and loading steps asynchronously using `asyncio` and `run_in_executor` for potentially blocking operations.
*   **Database Utilities**:
    *   `etl/db_utils.py` provides helper functions for database connection and DDL execution.
*   **Configuration**:
    *   File paths for sample data are defined in `main.py`.
    *   Database connection parameters are primarily sourced from environment variables (defaults provided in `etl/db_utils.py` and `docker-compose.yml`).
    *   Pydantic models in `etl/schemas.py` (for ETL) and `api/models.py` (for API) centralize data validation rules.
    *   DBT project configuration in `dbt_project/dbt_project.yml` and `dbt_project/profiles.yml`.
*   **Dockerization**:
    *   `Dockerfile` (root) for the Python ETL and dbt CLI application.
    *   `api/Dockerfile` for the FastAPI application.
    *   `docker-compose.yml` for easy multi-container setup (ETL app, API app, PostgreSQL database).
*   **Unit Tests (Python)**:
    *   Comprehensive unit tests for extraction, transformation, and mocked loading modules (`tests/`).

## 3. Code Structure

```
.
├── Dockerfile              # For building the Python ETL & DBT CLI container
├── docker-compose.yml      # For Docker Compose setup (ETL app, API app & database)
├── main.py                 # Main script to run the Python ETL pipeline
├── requirements.txt        # Python dependencies for ETL, DBT, and API
├── README.md               # This file
├── api/                    # FastAPI application
│   ├── Dockerfile          # Dockerfile for the API service
│   ├── main.py             # FastAPI app instance and root endpoint
│   ├── database.py         # SQLAlchemy setup and ORM models for API
│   ├── models.py           # Pydantic schemas for API requests/responses
│   ├── crud.py             # CRUD operations for the API
│   ├── dependencies.py     # API dependencies (e.g., get_db session) - (currently empty but structured)
│   └── routers/            # API endpoint routers
│       ├── __init__.py
│       ├── patients.py     # Patient and patient-specific biometric summary routes
│       └── biometrics.py   # Device reading and general biometric analytics routes
├── data/                   # Directory for input data files (created by main.py if not present)
│   ├── patients.json       # Sample patient data (generated by main.py)
│   └── device_readings.csv # Sample device readings data (generated by main.py)
├── dbt_project/            # DBT project for transformations and analytics
│   ├── dbt_project.yml     # DBT project configuration
│   ├── profiles.yml        # DBT connection profile (for Docker environment)
│   ├── models/             # DBT models
│   │   ├── sources/        # Source definitions (e.g., sources.yml)
│   │   ├── staging/        # Staging models (e.g., stg_patients.sql)
│   │   └── marts/          # Mart models (e.g., patient_biometric_summary.sql)
│   ├── seeds/              # Seed files (CSV for dbt seed) - currently empty
│   └── tests/              # DBT custom data tests - currently empty
├── etl/                    # Core Python ETL logic package
│   ├── __init__.py
│   ├── db_utils.py         # Database connection and DDL utilities
│   ├── extraction.py       # Data extraction functions
│   ├── loading.py          # Data loading functions (to PostgreSQL)
│   ├── schemas.py          # Pydantic schema definitions for ETL
│   └── transformation.py   # Data transformation and validation functions
└── tests/                  # Python unit tests for ETL
    ├── __init__.py
    ├── test_extraction.py
    ├── test_transformation.py
    └── test_loading.py     # Tests for Python loading logic (mocked DB)
```

## 4. Database Schema

The ETL pipeline loads data into the following PostgreSQL tables. DDL for these tables is executed automatically by the pipeline if they do not exist. These tables also serve as sources for the dbt project.

### `patients` Table
```sql
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
```

### `device_readings` Table
```sql
CREATE TABLE IF NOT EXISTS device_readings (
    id VARCHAR(255) PRIMARY KEY,
    patient_id VARCHAR(255) REFERENCES patients(id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ NOT NULL,
    glucose NUMERIC(8, 2),
    systolic_bp INTEGER,
    diastolic_bp INTEGER,
    weight NUMERIC(8, 2)
);

CREATE INDEX IF NOT EXISTS idx_device_readings_patient_id_timestamp ON device_readings(patient_id, timestamp);
```

### `error_records` Table
```sql
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
```
DBT models (e.g., `patient_biometric_summary`) will be created in the same database, typically in the schema defined in `dbt_project/profiles.yml` (defaulting to `public` or as configured).

## 5. Setup Instructions

### Prerequisites
*   Python 3.11+
*   pip (Python package installer)
*   Docker and Docker Compose (if using containerized approach)

### Installation
1.  Clone the repository (or ensure you have the code).
2.  Navigate to the project root directory.
3.  Install dependencies (primarily for local Python execution without Docker, as Docker handles its own dependencies):
    ```bash
    pip install -r requirements.txt
    ```

## 6. How to Run the Pipeline & Services

### Directly with Python (ETL Only)
*(Note: This runs only the Python ETL part. It requires a PostgreSQL instance to be running and accessible, with connection details matching environment variables or defaults in `etl/db_utils.py`. For the full system including API and dbt, using Docker Compose is recommended.)*

1.  Ensure you have completed the setup instructions.
2.  Set environment variables for your database if they differ from defaults (e.g., `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`).
3.  Run the main script from the project root directory:
    ```bash
    python main.py
    ```
4.  The script will execute the Python ETL pipeline. (DBT models are not run by this script. See Section 8 for running dbt.)

### Using Docker Compose (Recommended for Full System)

This project includes a `docker-compose.yml` file that sets up the Python ETL application (`app` service), the FastAPI application (`api` service), and a PostgreSQL database (`db` service). This is the recommended way to run the full application locally.

1.  **Ensure Docker Compose is installed.** (It's usually included with Docker Desktop).
2.  **Build and start all services**:
    Navigate to the project root directory and run:
    ```bash
    docker-compose up --build -d
    ```
    The `--build` flag ensures images are rebuilt if there are changes. The `-d` flag runs services in detached mode.
    This command will:
    *   Start the PostgreSQL database (`db` service).
    *   Start the Python ETL application (`app` service), which will run `python main.py` to load initial data.
    *   Start the FastAPI application (`api` service). The API will be available at `http://localhost:8000`.

3.  **To stop the services**:
    ```bash
    docker-compose down
    ```
    This will stop and remove the containers. The PostgreSQL data will persist in a Docker volume (`pgdata`) unless the volume is explicitly removed.

**Note on Service Interaction**:
- The Python ETL (`app` service) loads data into the PostgreSQL `db` service.
- The dbt CLI (run within the `app` service container) transforms data within the `db` service.
- The FastAPI application (`api` service) reads from and writes to the `db` service.

## 7. Example of Pipeline Execution (Python ETL)

When the `app` service starts (e.g., via `docker-compose up`), you will see output similar to the following for the Python ETL part (counts and specific messages may vary based on sample data and run conditions):

```
Running ETL Pipeline with PostgreSQL Integration...
Attempting to initialize database schema...
Successfully connected to PostgreSQL database.
DDL statements executed successfully.
Database schema initialized (or already exists).
Database connection closed.
Successfully connected to PostgreSQL database.
Created sample data: data/patients.json
Created sample data: data/device_readings.csv
Starting data extraction...
Extraction completed in X.XX seconds. Patients: 5, Devices: 7
Starting data transformation...
Transformation completed in Y.YY seconds. Valid Patients: A, Valid Readings: B, Transformation error records: C
Starting data loading into database...
Database loading completed in Z.ZZ seconds.

--- ETL Pipeline Summary ---
Total execution time: T.TT seconds
Patients extracted: 5
Device readings extracted: 7
Valid patients for DB: A
Valid device readings for DB: B
Transformation error records: C

--- Database Loading Summary ---
Successfully loaded patients to DB: A'
Successfully loaded device readings to DB: B'
Database Loading Errors (Data): [...] (List of errors if any)
Successfully loaded transformation error records to DB: C'
Database Loading Errors (Error Records): [...] (List of errors if any)
Database connection closed.
ETL Pipeline finished.
```
*(A, B, C, A', B', C', X, Y, Z, T are placeholders for actual numbers from an execution run.)*

### Verifying Data in PostgreSQL (After Python ETL)

After the Python ETL pipeline runs, you can connect to the PostgreSQL database to verify that data has been loaded into the source tables (`patients`, `device_readings`, `error_records`).

1.  **Connect to the PostgreSQL container**:
    Open a new terminal and run:
    ```bash
    docker-compose exec db psql -U etl_user -d etl_data
    ```
    Password: `etl_password`.

2.  **Example SQL Queries**:
    ```sql
    SELECT COUNT(*) FROM patients;
    SELECT COUNT(*) FROM device_readings;
    SELECT COUNT(*) FROM error_records;
    ```

## 8. DBT (Data Build Tool) Project

This project includes a DBT project located in the `dbt_project/` directory for transforming data within PostgreSQL *after* the Python ETL has loaded the source data.

### 8.1 Purpose
- Model data from source tables (`patients`, `device_readings`).
- Calculate derived metrics, such as biometric summaries per patient.
- Create new tables/views for analytics, reporting, or API use.

### 8.2 Structure
- `dbt_project.yml`: Main DBT project configuration.
- `profiles.yml`: Connection profiles (configured for Docker).
- `models/`:
  - `sources/sources.yml`: Defines dbt sources.
  - `staging/`: Staging models (e.g., `stg_patients.sql`).
  - `marts/`: Analytical models (e.g., `patient_biometric_summary.sql`).

### 8.3 Running DBT Commands

DBT commands are run from within the `app` container.

1.  **Ensure Docker Compose services are running** (as per Section 6).
2.  **Exec into the `app` container**:
    ```bash
    docker-compose exec app /bin/bash
    ```
3.  **Navigate to the DBT project directory (optional but good practice)**:
    ```bash
    cd dbt_project 
    ```
    (DBT_PROJECT_DIR and DBT_PROFILES_DIR are set, so commands work from `/app` too).

4.  **Test DBT Connection (optional)**:
    ```bash
    dbt debug 
    ```
5.  **Run DBT Models**:
    ```bash
    dbt run
    ```
6.  **Run DBT Tests**:
    ```bash
    dbt test
    ```
After `dbt run`, the `patient_biometric_summary` table will be created/updated. Query it via `psql` (e.g., `SELECT * FROM patient_biometric_summary LIMIT 5;`).

## 9. FastAPI Application (API)

The project includes a RESTful API built with FastAPI, located in the `api/` directory. This API allows interaction with the patient and device biometric data stored in the PostgreSQL database, including data generated by the ETL process and DBT transformations.

### 9.1 Running the API

The API service is managed by Docker Compose.

1.  **Start all services (including the API)**:
    If not already running from Section 6:
    ```bash
    docker-compose up --build -d
    ```
    The API will be available at `http://localhost:8000`.

2.  **Access API Documentation (Swagger UI)**:
    Once the API is running, interactive API documentation (Swagger UI) is available at:
    `http://localhost:8000/docs`
    And alternative ReDoc documentation at:
    `http://localhost:8000/redoc`

### 9.2 Available Endpoints

**Root:**
- `GET /`: Welcome message.

**Patients (`/patients`):**
- `GET /`: List all patients with pagination.
  - Query Parameters: `skip` (int, default 0), `limit` (int, default 10).
- `GET /{patient_id}`: Get details for a specific patient.
- `GET /{patient_id}/biometric_summary`: Get the DBT-calculated biometric summary for a specific patient.

**Biometrics (Device Readings & Analytics):**
- `GET /patients/{patient_id}/device_readings`: List device readings for a specific patient.
  - Query Parameters: `biometric_type` (str, optional, e.g., 'glucose', 'blood_pressure', 'weight'), `skip` (int, default 0), `limit` (int, default 10).
- `POST /patients/{patient_id}/device_readings`: Upsert (create or update) a device reading for a patient.
  - Request Body: JSON object based on `DeviceReadingCreate` schema (including `id` for the reading itself).
- `DELETE /device_readings/{device_reading_id}`: Delete a specific device reading by its ID.
- `GET /biometric_analytics`: List all pre-calculated biometric summaries for all patients (from DBT).
  - Query Parameters: `skip` (int, default 0), `limit` (int, default 10).

### 9.3 Example API Usage (curl)

**List Patients (first 2):**
```bash
curl -X GET "http://localhost:8000/patients/?skip=0&limit=2"
```

**Get Patient "p1" Details:**
```bash
curl -X GET "http://localhost:8000/patients/p1"
```

**Get Biometric Summary for Patient "p1":**
```bash
curl -X GET "http://localhost:8000/patients/p1/biometric_summary"
```

**List Device Readings for Patient "p1" (first 2 glucose readings):**
```bash
curl -X GET "http://localhost:8000/patients/p1/device_readings/?biometric_type=glucose&skip=0&limit=2"
```

**Upsert a Device Reading for Patient "p1":**
(Replace `reading_id_unique` with a unique ID for the reading)
```bash
curl -X POST "http://localhost:8000/patients/p1/device_readings" \
   -H "Content-Type: application/json" \
   -d '{
 "id": "reading_id_unique_007",
 "patient_id": "p1",
 "timestamp": "2023-05-10T14:30:00Z",
 "glucose": 105.5,
 "systolic_bp": 121,
 "diastolic_bp": 81,
 "weight": 70.2
}'
```
*(If the reading ID already exists, it will be updated. Ensure patient_id in body matches path)*

**Delete Device Reading "reading_id_unique_007":**
```bash
curl -X DELETE "http://localhost:8000/device_readings/reading_id_unique_007"
```
*(Expects a 204 No Content response if successful)*

**List All Biometric Analytics (first 2 summaries):**
```bash
curl -X GET "http://localhost:8000/biometric_analytics/?skip=0&limit=2"
```

## 10. Running Unit Tests (Python)

To run the Python unit tests for the ETL extraction and transformation logic, navigate to the project root directory on your host machine and execute:

```bash
python -m unittest discover -s tests -v
```

This command will discover and run all tests within the `tests` directory and provide verbose output. You should see all tests passing. The tests primarily cover extraction, transformation, and the database loading logic (using mocks).
```
