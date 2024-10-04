import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

@dag(
    dag_id='1_variable_scope_test',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': days_ago(1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='Variable scope test',
    schedule=None,
    catchup=False,
    tags=['example', 'test'],
)
def dag1():
    @task
    def task1(my_ts):
        log.info(my_ts)

    @task
    def task2(my_ts):
        log.info(my_ts)

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    task1(timestamp) >> task2(timestamp)

dag1()