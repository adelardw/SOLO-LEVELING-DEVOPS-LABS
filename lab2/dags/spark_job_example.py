from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime,timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 9),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


dag = DAG(
    'SparkDag',
    default_args=default_args,
    description='Pyspark Workflow',
    schedule_interval=timedelta(days=1),
    catchup=False
)


intro = BashOperator(
    task_id="intro",
    bash_command='echo "Spark Connection ..."',
    dag=dag,
)


spark_job = SparkSubmitOperator(task_id='Spark_Task_1', 
                                application='/opt/airflow/spark/spark_workflow.py',
                                conn_id='spark_local', 
                                dag=dag)

outro = BashOperator(
    task_id="outro",
    bash_command='echo "Well Done !"',
    dag=dag,
)

intro >> spark_job >> outro