
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators import PythonOperator
from airflow.hooks import RedisHook
from airflow.models import Variable


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

	

def sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval,n):
	device_slot = Variable.get("device_slot")
	redis_hook = RedisHook(redis_conn_id="redis")
	
	dag_subdag = DAG(
    		dag_id="%s.%s" % (parent_dag_name, child_dag_name),
    		schedule_interval=schedule_interval,
    		start_date=start_date,
  		)	
	#value = kwargs['ti'].xcom_pull(key='processed_data')
	#print kwargs	
	def format_data(**args):
		slot =args.get("params").get("slot") 		
		all_data=[]
		service_data_device_wise = list(redis_hook.rget("cpe_slot_%s" %str(slot)))
		#to map the DS
		datasource_mapping= {"device_name":"", "service_name":"", "machine_name":"","site_name":"", "data_source":"", "current_value":"", 			"min_value":"","max_value":"", "avg_value":"","warning_threshold":"","critical_threshold":"", "sys_timestamp":"", 			"check_timestamp":"","ip_address":"","severity":"","age":"","refer":""}
		device_list=[] # the container to hold the
		filter_service_list=[] #these services will not be avaialble rest all will be there in the dict 
		#service_data_device_wise = [value[i+1:i+31] for i in range(0,len(value),30)]
		#first_identifier = [i for i, s in enumerate(value) if 'kpi-snapshot' in s]	
		offset = 30 #remove this
		#TODO;the major culprit to be improved later is this
		#TODO:include Try Catch
		if len(filter_service_list) == 0 :
			Variable.set("filter_service_list_length",'0') # we get this list to get the actual service length which is to be processed so that slots can be made accordingly in Alert_Engine
		else:
			Variable.set("filter_service_list_length",str(len(filter_service_list)))
		thresholds = eval(Variable.get("cpe_services_list_thresholds"))
		for device_services in service_data_device_wise:
			#TODO:Add try catch here
			device_services_list = eval(device_services) #to convert from string to list
			datasource_mapping["device_name"] = device_services_list[3].strip().split(' ')[1]
			datasource_mapping["machine_name"] = device_services_list[3].strip().split(' ')[1]
			datasource_mapping["site_name"] = device_services_list[3].strip().split(' ')[1]
			datasource_mapping["sys_timestamp"] = datetime.utcnow().strftime('%B %d %Y - %H:%M:%S') #TODO:convert to Epoch
			datasource_mapping["check_timestamp"] = device_services_list[1].strip().split(' ')[1]
			datasource_mapping["ip_address"] = device_services_list[5].strip().split(' ')[1]
			if len(device_services_list) > 0:	
				for services in device_services_list:
					service_list = services.strip().split(' ')
					if service_list[0] not in filter_service_list:
						service_name = service_list[0].lower()
						service_value = service_list[1].lower()
						datasource_mapping["service_name"] = service_name
						datasource_mapping["current_value"] = service_value
						datasource_mapping["warning_threshold"]= thresholds["lte_cpe_"+service_name+":war"] if thresholds.has_key("lte_cpe_"+service_name+":war") else "" #TODO: Handle if the values are not set or unavalable using either try or if
						datasource_mapping["critical_threshold"]=thresholds["lte_cpe_"+service_name+":crit"]  if thresholds.has_key("lte_cpe_"+service_name+":crit") else "" #TODO: Same as above
						if thresholds.has_key("lte_cpe_"+service_name+":war") and thresholds.has_key("lte_cpe_"+service_name+":crit"): 
							datasource_mapping["severity"]= "OK" if service_value < thresholds["lte_cpe_"+service_name+":war"] else "critical" if  service_value > thresholds["lte_cpe_"+service_name+":crit"] else "warning"
						else:
							datasource_mapping["severity"]= "unknown"
						datasource_mapping["age"]= datetime.utcnow().strftime('%B %d %Y - %H:%M:%S')						
						device_list.append(datasource_mapping.copy()) #TODO: datasource_mapping deepcopy(datasource_mapping)
		print "++++++++++++++++++++++++++++++++++++++++++++++++++"
		print device_list
		print "++++++++++++++++++++++++++++++++++++++++++++++++++"
		#TODO: Send this to db 
		redis_hook.rpush("combined_slot_dict",device_list)
	
	for i in range(n):
		PythonOperator(
   		task_id="format_data_Task_%s"%(i),
  		provide_context=True,
 		python_callable=format_data,
		params={"slot":i,"redis_hook":redis_hook},
		dag=dag_subdag
	)
		
	
		
	return dag_subdag




