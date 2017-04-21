from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'wireless',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['vipulsharma144@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'correlate', default_args=default_args, schedule_interval=timedelta(1))

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='Recieved_Alarms',
    bash_command='sleep 2',
    dag=dag)

t2 = BashOperator(
    task_id='getStaticDataFromDB',
    bash_command='sleep 8',
    retries=3,
    dag=dag)

t3 = BashOperator(
    task_id='Classify_All_Data',
    bash_command='sleep 12',
    retries=3,
    dag=dag)

t4 = BashOperator(
    task_id='Add_Classified_Data_To_DB',
    bash_command='sleep 8',
    retries=3,
    dag=dag)

t5 = BashOperator(
    task_id='Get_Classified_Data_from_DB',
    bash_command='sleep 4',
    retries=3,
    dag=dag)

t6 = BashOperator(
    task_id='Correlate_Data',
    bash_command='sleep 10',
    retries=3,
    dag=dag)

t7 = BashOperator(
    task_id='Load_Data',
    bash_command='sleep 2',
    retries=3,
    dag=dag)


# templated_command = """
#     {% for i in range(5) %}
#         echo "{{ ds }}"
#         echo "{{ macros.ds_add(ds, 7)}}"
#         echo "{{ params.my_param }}"
#     {% endfor %}
# # """

# t3 = BashOperator(
#     task_id='templated',
#     bash_command=templated_command,
#     params={'my_param': 'Parameter I passed in'},
#     dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t3)
t5.set_upstream(t4)
t6.set_upstream(t5)
t7.set_upstream(t6)
