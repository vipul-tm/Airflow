from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators import PythonOperator
from airflow.hooks import RedisHook
from airflow.models import Variable
from subdags.format_utility import get_threshold
from subdags.format_utility import get_device_type_from_name
import logging
import traceback


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



redis_hook_4 = RedisHook(redis_conn_id="redis_hook_4")
rules = eval(Variable.get('rules'))

data_dict =  {'age': 'unknown',
		 'check_time': 'unknown',
		 'ds': 'unknown',
		 'host': 'unknown',
		 'ip_address': 'unknown',
		 'local_timestamp': 'unknown',
		 'cric': 'unknown', 
		 'cur': 'unknown',
		 'war':'unknown',
		 'refer': 'unknown',
		 'service': 'unknown',
		 'severity':'unknown',
		 'site': 'unknown'}
network_dict = {
				'site': 'unknown' ,
				'host': 'unknown',
				'service': 'unknown',
				'ip_address': 'unknown',
				'severity': 'unknown',
				'age': 'unknown',
				'ds': 'unknown',
				'data': 'unknown',
				'meta': 'unknown',
				'check_time': 'unknown',
				'local_timestamp': 'unknown' ,
				'refer':'unknown'
				}

def format_etl(parent_dag_name, child_dag_name, start_date, schedule_interval):
	network_slots = redis_hook_4.get_keys("nw_*")
	service_slots = redis_hook_4.get_keys("sv_*")
	
	dag_subdag_format = DAG(
    		dag_id="%s.%s" % (parent_dag_name, child_dag_name),
    		schedule_interval=schedule_interval,
    		start_date=start_date,
  		)
	def get_severity_values(service):
		all_sev = rules.get(service)
		sev_values = []
		for i in range(1,len(all_sev)+1):
			sev_values.append(all_sev.get("Severity"+str(i))[1].get("value"))
		return sev_values
	#TODO: Add EXOR iff required
	def evaluate_condition(rules,current_value):
		result =  'False'
		result_all = []
		operators = eval(Variable.get('operators')) #get operator Dict from 
		for i in range(1,len(rules),2):
			threshold_value = rules[i].get('value') #get threshold from rules dict
			operator = rules[i].get('operator') #get operator from rules
			symbol = operators.get(operator) #get symbol from dict
			if threshold_value != 'unknown':
				logging.info("\n Evaluating ")
				
				logging.info("Evaluating the Formula ---> "+str(current_value)+str(symbol)+str(threshold_value))
				try:
					if eval(current_value+symbol+threshold_value):
						result_all.append('True')
					else:
						result_all.append('False')
				except (NameError, SyntaxError):
					if eval('\''+current_value+'\''+symbol+'\''+threshold_value+'\''):
						result_all.append('True')
					else:
						result_all.append('False')
			else:
				result_all.append('False')

			try:
				logging.info(rules)
				logging.info("i="+str(i))
				if rules[i+1] == 'AND' or rules[i+1] == 'OR' and rules[i+1] != None:
					result_all.append(rules[i+1].lower())
				else:
					result_all.append("wrong_conjugator")
			except IndexError:
					logging.info('No Conjugator or the rule ended')
					continue
		logging.info("The Result After compiling booleans ---> "+ str(result_all))
		if len(result_all) == 1:
			result = eval(result_all[0])
		elif len(result_all) % 2 != 0:
			result = eval(" ".join(result_all))

		else:
			print ("Please Check the syntax of rules")
		logging.info("returning ; "+ str(result))
		return result

	def calculate_severity(service,cur):
		final_severity = []
		global rules 
		rules = eval(Variable.get('rules'))
		#TODO: Currently using loop to get teh Dic value can get hashed value provided the total severity for the devices remain fixed need to consult
		print service
		try:
			total_severities = rules.get(service) #TODO: Handle if service not found
			total_severities_len = len(total_severities)
		#Severity 1 will be the first to checked and should be the top priority i.e if 
		except TypeError:
			logging.info("The specified service "+service+" does not have a rule specified in reules variable")
			return 'unknown'
		for i in range(1,total_severities_len+1):
				sv_rules = total_severities.get("Severity"+str(i))
				if sv_rules[0]:
					current_severity = sv_rules[0]
				else:
					current_severity = 'unknown'
				result = evaluate_condition(sv_rules,cur)
				if result:	#final_severity =  final_severity.append(evaluate_condition(rules,cur)) #Later can be used to get all the SEV and then based upon priority decide Severity
					logging.info("The Final Result for Service "+service+" is " + str(result) +"having Val "+ str(cur) +" and Severity : "+ str(current_severity))
					return current_severity
				else:
					continue
				
		return -1



	def network_format(**kwargs):
		print "In Network Format"
		redis_queue_slot=kwargs.get('task_instance_key_str').split("_")[2:-3]
		redis_queue_slot="_".join(redis_queue_slot)
		#slot_data = redis_hook_4.rget(redis_queue_slot)
		slot_data = [[u'110556',u'10.171.132.2',0,1491822300,1491622173,u'rta=1.151ms;50.000;60.00;0;pl=0%;10;20;; rtmax=1.389ms;;;; rtmin=1.035ms;;;;']] * 40 
		network_list = []
		for slot in slot_data:
			network_dict = {
				'site': 'unknown' ,
				'host': 'unknown',
				'service': 'unknown',
				'ip_address': 'unknown',
				'severity': 'unknown',
				'age': 'unknown',
				'ds': 'unknown',
				'data': 'unknown',
				'meta': 'unknown',
				'check_time': 'unknown',
				'local_timestamp': 'unknown' ,
				'refer':'unknown'
				}
			try:
				device_type = get_device_type_from_name(slot[0]) #HANDLE IF DEVICE NOT FOUND
			except ValueError:
				logging.error("Couldn't find Hostmk dict need to calculate the thresholds Please run SYNC DAG")
				#TODO: Automatically run sync dag here if not found

			threshold_values = get_threshold(slot[-1])
			rt_min_cur = threshold_values.get('rtmin').get('cur')
			rt_max_cur = threshold_values.get('rtmax').get('cur')
			host_state = "up" if slot[2] else "false"
			network_dict['site'] = "_".join(redis_queue_slot.split("_")[1:4])
			network_dict['host'] = slot[0]
			network_dict['service'] = 'ping'
			network_dict['ip_address'] = slot[1]
			
			network_dict['age'] = slot[4]
			
			network_dict['check_time'] = slot[2]
			network_dict['local_timestamp'] = slot[3]
			device_type = get_device_type_from_name(slot[0])
			print slot[0]
			print device_type
			for data_source in threshold_values:
				value = threshold_values.get(data_source).get("cur")
				key=str(device_type+"_"+data_source)
				network_dict['severity'] = 	calculate_severity(key,value)
				network_dict['ds'] = data_source
				network_dict['refer'] = '' #TODO:  add logic for last down timestamop
			network_list.append(network_dict.copy())
				
		print network_list
		
	def service_format(**kwargs):
		print "In Service Format"
		redis_queue_slot=kwargs.get('task_instance_key_str').split("_")[2:-3]
		redis_queue_slot="_".join(redis_queue_slot)
		slot_data = redis_hook_4.rget(redis_queue_slot)
		try:
			for device_data in slot_data:
				#device_data = [u'1000', u'10.191.26.54', u'wimax_dl_cinr', 0, 1492076633, 1492075158, 1, u'dl_cinr=26;15;19;;']
				#			   [u'1', u'10.171.132.2', u'wimax_bs_temperature_acb', 0, 1492076618, 1491617765, 0, u'acb_temp=36;40;45;;']
				device_data = eval(device_data)
				ds_values = device_data[7].split('=')
				severity_war_cric  = get_severity_values(device_data[2])
				data_dict['host'] = device_data[0]
				data_dict['ip_address'] = device_data[1]
				data_dict['ds'] = ds_values[0] if len(ds_values) >= 1  else 'unknown'
				data_dict['check_time'] = device_data[3]
				data_dict['local_timestamp'] = device_data[3] #TODO: Forward in multiple of 5
				data_dict['service'] = device_data[2]
				data_dict['cur'] = ds_values[1].split(';')[0] if len(ds_values) > 1  else 'unknown'
				data_dict['war'] = severity_war_cric[1] if len(severity_war_cric) > 1 else 'unknown'
				data_dict['cric'] = severity_war_cric[0] if len(severity_war_cric) > 0 else 'unknown'
				#ds_values[1].split(';')[2]
				data_dict['severity'] =calculate_severity(device_data[2],ds_values[1].split(';')[0]) if len(ds_values) > 1  else 'unknown' #TODO: get data from static DB
				data_dict['age'] = device_data[5] #TODO: Calclate at my end change of severiaty
				data_dict['site'] = "_".join(redis_queue_slot.split("_")[1:4])

		except IndexError,e:
			print ("Some Problem with the dataset ---> \n"+str(device_data))	
			print ("SLOT ---> " + redis_queue_slot)
			traceback.print_exc()
		except Exception:
			traceback.print_exc()


	for redis_key in network_slots:
		if not redis_key:
			break
		task_name = redis_key.split("_")
		task_name.append("format")
		name = "_".join(task_name)
		PythonOperator(
			task_id="%s"%name,
			provide_context=True,
			python_callable=network_format,
			#params={"ip":machine.get('ip'),"port":site.get('port')},
			dag=dag_subdag_format
			)
	


	for redis_key in service_slots:
		if not redis_key:
			break

		task_name = redis_key.split("_")
		task_name.append("format")
		name = "_".join(task_name)
		
		PythonOperator(
			task_id="%s"%name,
			provide_context=True,
			python_callable=service_format,
			#params={"ip":machine.get('ip'),"port":site.get('port')},
			dag=dag_subdag_format
			)
	return 	dag_subdag_format
