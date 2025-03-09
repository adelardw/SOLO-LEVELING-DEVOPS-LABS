from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 9),
    "retries": 1}


dag = DAG(
    'SparkDags',
    default_args=default_args,
    description='Work with spark')


spark_job = SparkSubmitOperator(task_id='Spark_Task_1', 
                                application='/opt/airflow/spark/spark_workflow.py',
                                conn_id='spark_conn_id_1', 
                                dag=dag)


spark_job