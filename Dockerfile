# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt /app/

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code into the container at /app
COPY . /app/

# Ensure the sample data directory exists (though main.py also creates it)
RUN mkdir -p /app/data

# Command to run the application
# This will execute the main.py script which runs the ETL pipeline
CMD ["python", "main.py"]
