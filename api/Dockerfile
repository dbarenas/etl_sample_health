# api/Dockerfile
ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-slim

LABEL maintainer="Jules AI Agent"
LABEL description="Dockerfile for the FastAPI application providing patient and biometrics API."

WORKDIR /app

# Install system dependencies needed for psycopg2 (same as the main Dockerfile)
# This is needed because we copy requirements.txt from root which includes psycopg2
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

# Copy only the requirements file first to leverage Docker cache
COPY ../requirements.txt /app/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the API application code into the container
# It copies the content of the 'api' directory from the host 
# (where this Dockerfile is located) into '/app/api' in the container.
# Or, more simply, if the build context is set to the 'api' directory,
# COPY . . would copy everything from 'api' to '/app'.
# Let's assume the build context in docker-compose will be '.', 
# so we copy the specific 'api' subfolder.
COPY ./api /app/api 
# We also need the etl/schemas.py if Pydantic models in api/models.py import from it,
# or if any other shared utilities are needed.
# For now, api/models.py should be self-contained or use relative imports if shared models are structured.
# Let's assume api models are self-contained or will be adjusted if shared.
# If etl.schemas is needed by api (e.g. for shared Pydantic models), that needs to be copied too.
# For now, let's ensure the API can run with its own code + what's in requirements.txt.
# The crud.py, models.py etc. reference 'from . import ...' which is fine if WORKDIR is /app
# and the main.py is in /app/api/main.py.
# The CMD below assumes main:app is findable.

# Expose the port the app runs on
EXPOSE 8000

# Command to run the Uvicorn server
# This will look for an 'app' instance in 'api.main' module.
# The WORKDIR is /app. If main.py is in /app/api/main.py, then module is api.main
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
