from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from pendulum import timezone


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1, tzinfo=timezone('Asia/Seoul')),
}

with DAG(
    default_args=default_args,
    dag_id='postgres_test',
    schedule=None,
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    pg = PostgresOperator(task_id='pg',
                          sql='select 1',
                          postgres_conn_id='local_postgres')

    start >> pg >> end