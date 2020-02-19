from src.core.execute import core_aggregation, core_db_insert_to_db, core_get_data
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from src.utils.db import create_connection_object


DB_CONN = create_connection_object('postgres_default')
# SCHEDULE_INTERVAL = '*/10 * * * *'

default_args = {
    'owner': 'Amal',
    'start_date': datetime(2020, 1, 24, 10, 00, 00),
    'concurrency': 1,
    'retries': 0
}

DAG_VERSION = 'MovieRatings.0'

dag = DAG(DAG_VERSION
          , catchup=False
          , default_args=default_args
          , schedule_interval='*/10 * * * *')

get_data = PythonOperator(
    task_id='get_data',
    python_callable=core_get_data,
    retries=0,
    provide_context=True,
    dag=dag
)

aggregation = PythonOperator(
    task_id='aggregation',
    python_callable=core_aggregation,
    retries=0,
    provide_context=True,
    dag=dag
)

db_insert_to_db = PythonOperator(
    task_id='db_insert_to_db',
    python_callable=core_db_insert_to_db,
    op_args=[DB_CONN],
    retries=0,
    provide_context=True,
    dag=dag
)

print("starting")

get_data >> aggregation >> db_insert_to_db

print("done")