# Choose a version that matches your setup; 2.9.x is current stable line
FROM apache/airflow:2.9.3-python3.11

# Install system deps as root (for scipy/sklearn builds, SSL, etc.)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    curl \
    libssl-dev \
    libffi-dev \
    python3-dev \
    libpq-dev \
 && rm -rf /var/lib/apt/lists/*

# Back to the airflow user BEFORE running pip (Airflow policy)
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Keep default CMD/ENTRYPOINT from base image

