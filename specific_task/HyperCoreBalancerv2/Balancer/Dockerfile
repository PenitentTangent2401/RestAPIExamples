FROM python:3.11-slim

WORKDIR /app

# Install build dependencies required by Prophet
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your scripts into the container
COPY HyperCore_balancer.py .
COPY predictive_engine.py .
COPY config_db.py .

# Run the balancer unbuffered so logs show up immediately in Docker
CMD ["python", "-u", "HyperCore_balancer.py"]