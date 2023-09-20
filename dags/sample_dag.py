from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime

from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
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

    ssh_op = SFTPOperator(
        task_id='ssh_op',
        ssh_conn_id='source_ssh',
        local_filepath='/opt/airflow/dags/sample_dag.py',
        remote_filepath='sample_dag.py',
        operation='put',
        create_intermediate_dirs=True,
    )


    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    start >> task_1() >> ssh_op >> end

sample_dag()