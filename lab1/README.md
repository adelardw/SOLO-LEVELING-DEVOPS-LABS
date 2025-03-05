# First one, you need download docker-compose.yml for airflow:

```
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.5/docker-compose.yaml'
```
# The next step is .dockerfile (dont remember download docker on you OS):

```
FROM apache/airflow:2.7.1
WORKDIR /opt/airflow
COPY dags /opt/airflow/dags
RUN pip install scikit-learn==1.3.2
```

# Prepare Docker-Compose:   

1. Cut redis, airflow-worker, airflow-triggerer, airflow-cli, flower.
2. Change AIRFLOW_CORE_EXECUTOR from CeleryExecutor to LocalExecutor
3. Change `image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.7.1}` to `build: .` 
or `image: ${AIRFLOW_IMAGE_NAME:-your_image_name}`

# How to build:
1. mkdir dags and add your tasks in this folder
2. docker-compose -p 'your project name' up -d 
3. check running containers: docker ps (if healthy - good, else -> wait)
4. if you want to stop: docker-compose -p 'your project name' stop 
`


