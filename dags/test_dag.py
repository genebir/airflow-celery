from airflow import DAG
from selenium import webdriver
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from pendulum import timezone

default_args = {
    'owner': 'airflow',
    'schedule': None,
    'catchup': False
}

def selenium_test():
    with webdriver.Remote('http://172.20.0.4:4444/wd/hub') as driver:
        driver.get('http://www.naver.com')
        print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
        print(driver)
        print('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
        print(driver.title)
        print('>>>>>>>>>>>>>>>>>>>>>', driver.title)
        driver.quit()
        pass


with DAG(
    default_args=default_args,
    dag_id='test_dag',
    schedule=None,
    tags=['test'],
    start_date=datetime(2023, 1, 1).astimezone(tz=timezone('Asia/Seoul'))
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    selen = PythonOperator(
        task_id='selenium_test',
        python_callable=selenium_test
    )

    start >> selen >> end
