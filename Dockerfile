# Course image: official Airflow 3.1.8 + the libraries the pipeline needs.
# A custom image is preferable to installing packages every time a container starts.
FROM apache/airflow:3.1.8

COPY requirements.txt /requirements.txt

# Pinning "apache-airflow==3.1.8" lets pip keep Airflow's own dependency set consistent.
RUN pip install --no-cache-dir "apache-airflow==3.1.8" -r /requirements.txt
