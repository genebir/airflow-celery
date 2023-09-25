from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from pendulum import timezone
from operators.api import ApiOperator
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1, tzinfo=timezone('Asia/Seoul')),
}

def make_df(**context):
    ti = context['ti']
    df = pd.DataFrame.from_records(ti.xcom_pull(task_ids='api')['row'])
    print(df)



with DAG(
    default_args=default_args,
    dag_id='collection_test_',
    schedule=None,
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    api = ApiOperator(task_id='api',
                      service='bikeList',
                      start_index=1,
                      end_index=1000,
                      key='rentBikeStatus',)
    mk_df = PythonOperator(task_id='df',
                        python_callable=make_df,
                        provide_context=True)


    start >> api >> mk_df >> end