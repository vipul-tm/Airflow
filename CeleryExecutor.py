from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime, timedelta
import time
import requests
from celery_config import app

default_args = {
    'owner': 'wireless',
    'depends_on_past': False,
    'start_date': datetime(2017, 2, 24),
    'email': ['vipulsharma144@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'celeryExecutor', default_args=default_args, schedule_interval='*/2 * * * *')

@app.task
def fetch_url(url):
    resp = requests.get(url)
    print resp.status_code
    print "--------The status code------------"
    return 0

def serial_urls(urls):
    for url in urls:
        fetch_url.delay(url)

# t1, t2 and t3 are examples of tasks created by instantiating operators

t1 = PythonOperator(
    task_id='celery_fetchURL',
    provide_context=False,
    python_callable=serial_urls,
    op_args=[["http://google.com","http://facebook.com","http://hotmail.com"]],
    dag=dag)

