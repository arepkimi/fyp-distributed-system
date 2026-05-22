# Use a slim, stable Python base image to keep the container lightweight
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies required by OpenCV binaries
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements list first to utilize Docker's caching mechanism
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the dashboard raw source files
COPY . .

# Expose the Flask default ingress socket port
EXPOSE 5000

# Run the Flask app web gateway bound to 0.0.0.0
CMD ["python", "app.py"]