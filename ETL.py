import os
import socket
from airflow import DAG
from airflow.contrib.hooks import SSHHook
from airflow.operators import PythonOperator
from airflow.operators import DummyOperator
from airflow.operators import BashOperator
from airflow.operators import BranchPythonOperator
from airflow.hooks import RedisHook
from airflow.hooks.mysql_hook import MySqlHook
from datetime import datetime, timedelta	
from airflow.models import Variable
from airflow.operators import TriggerDagRunOperator
from airflow.operators.subdag_operator import SubDagOperator
from subdags.network import network_etl
from subdags.service import service_etl
from subdags.format import format_etl
from  etl_tasks_functions import get_required_static_data
from etl_tasks_functions import init_etl
import itertools
import socket
import sys
import time
import re
import random
import logging
import traceback
import os
import json
import utility

#TODO: Commenting 
#######################################DAG CONFIG####################################################################################################################

default_args = {
    'owner': 'wireless',
    'depends_on_past': False,
    'start_date': datetime(2017, 03, 30,13,00),
    'email': ['vipulsharma144@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'provide_context': True,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
     
}
#redis_hook = RedisHook(redis_conn_id="redis_4")
PARENT_DAG_NAME = "ETL"
CHILD_DAG_NAME_NETWORK = "NETWORK"
CHILD_DAG_NAME_SERVICE = "SERVICE"
CHILD_DAG_NAME_FORMAT = "FORMAT"

main_etl_dag=DAG(dag_id=PARENT_DAG_NAME, default_args=default_args, schedule_interval='@once')

#######################################DAG Config Ends ####################################################################################################################

    






#######################################TASKS####################################################################################################################

network_etl = SubDagOperator(
    subdag=network_etl(PARENT_DAG_NAME, CHILD_DAG_NAME_NETWORK, datetime(2017, 2, 24),"@once"),
    task_id=CHILD_DAG_NAME_NETWORK,
    dag=main_etl_dag
	)
service_etl = SubDagOperator(
    subdag=service_etl(PARENT_DAG_NAME, CHILD_DAG_NAME_SERVICE, datetime(2017, 2, 24),"@once"),
    task_id=CHILD_DAG_NAME_SERVICE,
    dag=main_etl_dag,
	)
format_etl = SubDagOperator(
    subdag=format_etl(PARENT_DAG_NAME, CHILD_DAG_NAME_FORMAT, datetime(2017, 2, 24),"@once"),
    task_id=CHILD_DAG_NAME_FORMAT,
    dag=main_etl_dag,
	)
get_static_data = PythonOperator(
    task_id="getThresholds_severity",
    provide_context=False,
    python_callable=get_required_static_data,
    #params={"redis_hook_2":redis_hook_2},
    dag=main_etl_dag)
initiate_etl = PythonOperator(
    task_id="Initiate",
    provide_context=False,
    python_callable=init_etl,
    #params={"redis_hook_2":redis_hook_2},
    dag=main_etl_dag)
#get_static_data >> format_etl