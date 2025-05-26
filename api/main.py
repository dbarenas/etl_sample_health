# api/main.py
from fastapi import FastAPI
from .database import engine, Base, create_db_tables # Import engine and Base for table creation if needed
from .routers import patients, biometrics

# This is the line that could create tables based on SQLAlchemy models.
# Given ETL and DBT manage primary tables, this should be used with caution or selectively.
# For now, we call our placeholder `create_db_tables` which currently does nothing destructive.
# If API were to manage its own tables, this would be Base.metadata.create_all(bind=engine).
# create_db_tables() # Call the function from database.py - currently a placeholder

app = FastAPI(
    title="Patient Data and Biometrics API",
    description="API for managing patient data, device readings, and viewing biometric analytics.",
    version="1.0.0"
)

# Include routers
app.include_router(patients.router)
app.include_router(biometrics.router) # This will include all routes from biometrics.py

@app.on_event("startup")
async def startup_event():
    """
    Actions to perform on application startup.
    - Ensure database tables are created (if API were responsible for any).
    """
    print("FastAPI application startup...")
    # The function create_db_tables() in database.py currently just prints a message.
    # If API were to manage its own tables, actual table creation logic would go there
    # and be invoked here, e.g., Base.metadata.create_all(bind=engine).
    # Since ETL and DBT manage the schema for shared tables, we keep this passive for now.
    create_db_tables() 
    print("Startup event complete. API is ready.")

@app.get("/", tags=["Root"])
async def read_root():
    """
    Root endpoint providing a welcome message.
    """
    return {"message": "Welcome to the Patient Data and Biometrics API. Visit /docs for API documentation."}

# To run this app (from the root directory, assuming uvicorn is installed):
# uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
# This command will be part of the Dockerfile for the api service.
