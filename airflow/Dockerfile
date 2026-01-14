FROM apache/airflow:2.10.0-python3.11

# Copy and install your project requirements
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy application code (optional - can also mount as volume)
# COPY app /opt/airflow/app

USER airflow