from airflow.hooks.postgres_hook import PostgresHook
import logging
import traceback


def create_connection_object(connection_id: str):

    try:
        pg_hook = PostgresHook(postgres_conn_id=connection_id)
        connection = pg_hook.get_connection(connection_id)
        logging.info("Connection: {0}".format(connection))
        connection_uri = 'postgresql+psycopg2://{c.login}:{c.password}@{c.host}:{c.port}/{c.schema}'.format(c=connection)
        return connection_uri
    except Exception as e:
        logging.error("Failed to initialize db connection. {}".format(e))
        logging.error(traceback.format_exc())