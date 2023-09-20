from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import os
from airflow.utils.task_group import TaskGroup
import shutil
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from pendulum import timezone

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1).astimezone(tz=timezone('Asia/Seoul')),
    'catchup': False,
}

@dag(default_args=default_args, schedule_interval=None, tags=['example'])
def sample_dag():

    file_list = os.listdir('/opt/airflow/data')
    local_to_gcs_group = TaskGroup(group_id='LOCAL_TO_GCS_GROUP')
    for _ in file_list:
        if _.startswith('.'):
            continue
        if _.startswith('old'):
            continue
        _task_id = _.split('.')[0].replace('(','_').replace(')','').replace(' ','_')
        LocalFilesystemToGCSOperator(
            task_id=f'local_to_gcs_{_task_id}',
            src=f'/opt/airflow/data/{_}',
            dst=f'data/{_}',
            bucket='gov-source-data',
            task_group=local_to_gcs_group
        )
    @task(task_id='move_to_old')
    def move_to_old():
        for _ in file_list:
            if _.startswith('.'):
                continue
            if _.startswith('old'):
                continue
            shutil.move(f'/opt/airflow/data/{_}', f'/opt/airflow/data/old/{_}')

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    start >> local_to_gcs_group >> move_to_old() >> end

sample_dag()