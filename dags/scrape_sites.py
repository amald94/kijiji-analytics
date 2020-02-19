from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from src.utils.db import create_connection_object
from src.webscraping.kijiji import getUrls, insert_adurls
from src.utils.variables import no_pages


DB_CONN = create_connection_object('postgres_default')
DAG_VERSION = 'WebScraper_1.0'
no_pages = int(no_pages)

default_args = {
    'owner': 'Amal',
    'start_date': datetime(2020, 2, 14, 10, 00, 00),
    'concurrency': 1,
    'retries': 0
}

dag = DAG(DAG_VERSION
          , catchup=False
          , default_args=default_args
          , schedule_interval='*/10 * * * *')

get_url_kijiji = PythonOperator(
    task_id='get_url_kijiji',
    python_callable=getUrls,
    op_args=[no_pages],
    retries=0,
    provide_context=True,
    dag=dag
)

adurls_insert_to_db = PythonOperator(
    task_id='adurls_insert_to_db',
    python_callable=insert_adurls,
    op_args=[DB_CONN],
    retries=0,
    provide_context=True,
    dag=dag
)


get_url_kijiji >> adurls_insert_to_db