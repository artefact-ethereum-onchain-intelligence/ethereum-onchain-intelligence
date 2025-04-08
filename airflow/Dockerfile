FROM apache/airflow:2.10.5

ARG AIRFLOW_VERSION=2.10.5

WORKDIR /opt/airflow

COPY requirements.txt .

RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
