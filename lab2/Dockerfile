FROM apache/airflow:2.7.1
WORKDIR /opt/airflow

USER root
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless

USER airflow
COPY ./dags/* ./dags/
COPY ./spark/* ./spark/

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.10 \
    apache-airflow-providers-openlineage==1.9.0 \