from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import sqlalchemy
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from dateutil.relativedelta import relativedelta
from pendulum import timezone
from operators.api import ApiOperator
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1, tzinfo=timezone('Asia/Seoul')),
}

def make_df(engine: sqlalchemy.engine,
            **context):
    ti = context['ti']
    df = pd.DataFrame.from_records(ti.xcom_pull(task_ids='api')['row'])
    df = df['RENT_NM',
            'RENT_TYPE',
            'STATION_NO',
            'GENDER_CD',
            'AGE_TYPE',
            'USE_CNT',
            'EXER_AMT',
            'CARBON_AMT',
            'MOVE_METER',
            'MOVE_TIME']

    df.to_sql(
        name='pblc_cyc_mthly_usg',
        con=engine,
        index=False,
        if_exists='append'
    )

    print(df)



with DAG(
    default_args=default_args,
    dag_id='collection_test_',
    schedule='00 00 30 * *',
) as dag:
    base_ym = (datetime.now() - relativedelta(months=1)).strftime('%Y%m')

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    postgres_hook = PostgresHook(postgres_conn_id='local_postgres')

    api = ApiOperator(task_id='api',
                      service='bikeList',
                      start_index=1,
                      end_index=1000,
                      key='rentBikeStatus',
                      date=base_ym)

    delete_data = PostgresOperator(task_id='delete_data',
                                   postgres_hook=postgres_hook,
                                   sql=f"delete from pblc_cyc_mthly_usg where base_ym = '{base_ym}'")

    mk_df = PythonOperator(task_id='df',
                           python_callable=make_df,
                           op_kwargs={'engine': postgres_hook.get_sqlalchemy_engine()}
                           )

    start >> api >> delete_data >> mk_df >> end