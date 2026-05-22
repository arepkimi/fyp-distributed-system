# Use a slim, stable Python base image matching our stack topology
FROM python:3.10-slim

# Set the active execution path inside the container boundaries
WORKDIR /app

# Install headless Linux media rendering toolkit dependencies for OpenCV
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Copy your packages checklist first to exploit layer cache speeds
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all code scripts and precompiled gRPC stubs directly into /app
COPY . .

# Expose the explicit gRPC listener port mapped in worker.py
EXPOSE 50053

# Boot up the gRPC worker node server process
CMD ["python", "worker.py"]