FROM apache/airflow:2.7.1-python3.9

# Optional OS deps
USER root
RUN apt-get update && apt-get install -y gcc python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages as airflow user (required by Airflow image)
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
