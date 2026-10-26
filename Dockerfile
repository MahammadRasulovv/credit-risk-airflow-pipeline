FROM apache/airflow:3.1.0

USER root

RUN apt-get update && \
    apt-get install -y git bash curl unzip openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
