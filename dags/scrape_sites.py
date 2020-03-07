from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from src.utils.db import create_connection_object
from src.webscraping.kijiji_urls import getUrls, insert_adurls
from src.utils.variables import no_pages
from src.webscraping.kijiji_details import scrape_innerdata
from src.core.dbcreate import create_db
from src.model.rent_prediction import getData_fromDB

DB_CONN = create_connection_object('postgres_default')
DAG_VERSION = 'KIJIJI-Scraper_1.3'
no_pages = int(no_pages)

default_args = {
    'owner': 'Amal',
    'start_date': datetime(2020, 2, 28, 10, 00, 00),
    'concurrency': 1,
    'retries': 0
}

dag = DAG(DAG_VERSION
          , catchup=False
          , default_args=default_args
          , schedule_interval='*/10 * * * *')

# get_url_kijiji = PythonOperator(
#     task_id='get_url_kijiji',
#     python_callable=getUrls,
#     op_args=[no_pages],
#     retries=0,
#     provide_context=True,
#     dag=dag
# )
#
# create_tables = PythonOperator(
#     task_id='create_tables',
#     python_callable=create_db,
#     op_args=[DB_CONN],
#     retries=5,
#     provide_context=True,
#     dag=dag
# )
#
# adurls_insert_to_db = PythonOperator(
#     task_id='adurls_insert_to_db',
#     python_callable=insert_adurls,
#     op_args=[DB_CONN],
#     retries=0,
#     provide_context=True,
#     dag=dag
# )
#
# get_innerdata = PythonOperator(
#     task_id='inner_data',
#     python_callable=scrape_innerdata,
#     op_args=[DB_CONN],
#     retries=5,
#     provide_context=True,
#     dag=dag
# )

prepare_data = PythonOperator(
    task_id='prepare_data',
    python_callable=getData_fromDB,
    op_args=[DB_CONN],
    retries=5,
    provide_context=True,
    dag=dag
)





# create_tables >> get_url_kijiji >> adurls_insert_to_db
#
# adurls_insert_to_db >> get_innerdata >> prepare_data

prepare_data