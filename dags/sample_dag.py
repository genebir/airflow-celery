from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from pendulum import timezone

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1).astimezone(tz=timezone('Asia/Seoul')),
    'catchup': False,
}

@dag(default_args=default_args, schedule_interval=None, tags=['example'])
def sample_dag():
    @task(task_id='task_1')
    def task_1():
        print('task_1')

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    start >> task_1() >> end

sample_dag()