from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score
import pickle
import os
import logging
from time import perf_counter


log = logging.getLogger("airflow.task")

def prepare_data():
    
    X, y = load_iris(return_X_y=True)
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, train_size=0.8, random_state=42)
    
    log.info('The IRIS dataset has been split on train and test: '
                 f'Train size {X_train.shape[0]}; Test size {X_test.shape[0]}' )
    
    return {"X_train": X_train, "X_test": X_test, "y_train": y_train, "y_test": y_test}


def train_model(**kwargs):
    log.info(f'Input Kwargs: {kwargs}')
    
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='prepare_data')
    
    X_train = data["X_train"]
    y_train = data["y_train"]
    
    log.info(f'Decision Tree Train')
    model = DecisionTreeClassifier(random_state=42)
    start_time = perf_counter()
    model.fit(X_train, y_train)
    fit_time = perf_counter() - start_time
    
    log.info(f'Decision tree model training time: {fit_time}')
    
    model_path = "iris_model.pkl"
    with open(model_path, "wb") as f:
        pickle.dump(model, f)
    
    return model_path


def validate_model(**kwargs):
    
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='prepare_data')
    model_path = ti.xcom_pull(task_ids='train_model')
    
    X_test = data["X_test"]
    y_test = data["y_test"]
    
    
    with open(model_path, "rb") as f:
        model = pickle.load(f)
    
    
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    log.info(f"Accuracy: {accuracy:.2f}")
    
    return accuracy


def deploy_model(**kwargs):
    
    ti = kwargs['ti']
    model_path = ti.xcom_pull(task_ids='train_model')
    accuracy = ti.xcom_pull(task_ids='validate_model')
    
    
    if accuracy >= 0.9:  
        
        production_path = "/production/iris_model.pkl"
        os.rename(model_path, production_path)
        log.info(f"Model has been saved by path: {production_path}")
    else:
        log.info("Accuracy threshold greater that accuracy value, model no saved")



dag = DAG(
    'ml_pipeline',
    default_args={
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 3),
    'retries': 1},
    description='ML Pipeline: Подготовка данных, обучение, валидация, деплой',
    schedule_interval='@daily')


prepare_data_task = PythonOperator(
    task_id='prepare_data',
    python_callable=prepare_data,
    dag=dag)

train_model_task = PythonOperator(
    task_id='train_model',
    python_callable=train_model,
    provide_context=True,  
    dag=dag)

validate_model_task = PythonOperator(
    task_id='validate_model',
    python_callable=validate_model,
    provide_context=True,  
    dag=dag)

deploy_model_task = PythonOperator(
    task_id='deploy_model',
    python_callable=deploy_model,
    provide_context=True,  
    dag=dag)


prepare_data_task >> train_model_task >> validate_model_task >> deploy_model_task