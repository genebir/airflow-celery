from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from dateutil.relativedelta import relativedelta
from pendulum import timezone
from operators.api import ApiOperator
from utils.rdb import Rdb
from sqlalchemy import create_engine
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1, tzinfo=timezone('Asia/Seoul')),
}

def make_df(**context):
    ti = context['ti']
    df = pd.DataFrame.from_records(ti.xcom_pull(task_ids='api')['row'])
    print(df.columns)
    df = df[['RENT_NM',
            'RENT_TYPE',
            'STATION_NO',
            'AGE_TYPE',
            'USE_CNT',
            'EXER_AMT',
            'CARBON_AMT',
            'MOVE_METER',
            'MOVE_TIME']]
    print(pd.isnull(df))

    data = list(df.to_records(index=False))
    print(data)
    rdb = Rdb()
    rdb.execute_values(table='pblc_cyc_mthly_usg',
                       data=data)
    print(data)



with DAG(
    default_args=default_args,
    dag_id='CO_PBLC_CYC_MTHLY_USG',
    schedule='00 00 30 * *',
    catchup=True,
) as dag:
    get_base_ym = PythonOperator(task_id='T.GET_BASE_YM',
                                 python_callable=lambda: (datetime.now() - relativedelta(months=1)).strftime('%Y%m'))

    start = EmptyOperator(task_id='START')
    end = EmptyOperator(task_id='END')

    api = ApiOperator(task_id='T.GET_DATA_FROM_API',
                      service='tbCycleRentUseMonthInfo',
                      start_index=1,
                      end_index=1000,
                      key='cycleRentUseMonthInfo',
                      date='{{ ti.xcom_pull(task_ids="get_base_ym") }}')

    delete_data = PostgresOperator(task_id='T.DELETE_DATA_FROM_RDB',
                                   postgres_conn_id='local_postgres',
                                   sql=f"""delete from pblc_cyc_mthly_usg where base_ym = '{{ ti.xcom_pull(task_ids="get_base_ym") }}'""")

    mk_df = PythonOperator(task_id='T.INSERT_INTO_RDB',
                           python_callable=make_df
                           )

    start >> get_base_ym >> api >> delete_data >> mk_df >> end