import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

@dag(
    dag_id='2_xcom_test',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': days_ago(1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='XCom test',
    schedule=None,
    catchup=False,
    tags=['example', 'test'],
)
def dag1():
    @task
    def task0_my_ts():
        return datetime.now().strftime("%Y%m%d-%H%M%S")

    @task
    def task1(my_ts):
        log.info(my_ts)

    @task
    def task2(my_ts):
        log.info(my_ts)

    timestamp = task0_my_ts()
    task1(timestamp) >> task2(timestamp)

dag1()