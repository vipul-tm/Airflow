from celery import Celery
import requests

app = Celery('airflow.executors.celery_executor', broker='redis://localhost:6379/0')

