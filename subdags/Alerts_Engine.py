
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators import PythonOperator
from airflow.hooks import RedisHook
from airflow.models import Variable
import itertools
from itertools import izip_longest


default_args_Alerts = {
    'owner': 'wireless',
    'depends_on_past': False,
    'start_date': datetime(2017, 03, 30,13,00),
    'email': ['vipulsharma144@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'provide_context': True,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

	

def Alerts_Engine(parent_dag_name, child_dag_name, start_date, schedule_interval,n):
	device_slot = Variable.get("device_slot")
	redis_hook = RedisHook(redis_conn_id="redis")
	total_services = int(Variable.get("total_services"))
	excluded_services = int(Variable.get("filter_service_list_length"))
	actual_services = 0
	try:
		actual_services = int(total_services - excluded_services)
		#TODO: Prevent from have excluded services > total services 
	except Exception:
		print("Unable to get actual services to e processed")	
	Alerts_Engine_subdag = DAG(
    		dag_id="%s.%s"%(parent_dag_name, child_dag_name),
    		schedule_interval=schedule_interval,
    		start_date=start_date,
  		)	
	
	def process_thresholds(**kwargs):
		print ("In Process_Threshold")
		print ("===================================")
		print(kwargs['params']['data'])





########################################################TASKS#####################################################################
	service_data_device_wise = list(redis_hook.rget("combined_slot_dict"))
	group_iter = [iter(service_data_device_wise)]*int(device_slot) * actual_services
 	device_slot_data = list(([e for e in t if e !=None] for t in itertools.izip_longest(*group_iter)))
	
	if len(device_slot_data) > 0:
		i=0
		for slot in device_slot_data:
			PythonOperator(
   			task_id="Check_Severity_change_%s"%(i),
  			provide_context=True,
 			python_callable=process_thresholds,
			params={"slot":i,"redis_hook":redis_hook,"data":slot},
			dag=Alerts_Engine_subdag
		)
			i=i+1
	#for i in range(n):
	#		PythonOperator(
   	#		task_id="Check_Severity_change_%s"%(i),
  	#		provide_context=True,
 	#		python_callable=process_thresholds,
	#		params={"slot":i,"redis_hook":redis_hook},
	#		dag=Alerts_Engine_subdag
	#	)

	return Alerts_Engine_subdag




