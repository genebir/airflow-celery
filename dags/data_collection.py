from airflow import DAG
from pendulum import timezone
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from operators.subway import SubwayOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1).astimezone(tz=timezone('Asia/Seoul')),
    'catchup': False,
}

with DAG(
    default_args=default_args,
    dag_id='subway_dag',
    schedule=None,
    tags=['subway', 'test']
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    subway = SubwayOperator(task_id='subway', subway_name='1호선')

    start >> subway >> end