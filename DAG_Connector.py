from airflow import DAG
from airflow.contrib.hooks import SSHHook
from airflow.operators import PythonOperator
from airflow.contrib.hooks import SSHParmikoHook
from airflow.operators import DummyOperator
from airflow.operators import BashOperator
from airflow.operators import BranchPythonOperator
from airflow.hooks import RedisHook
from datetime import datetime, timedelta
from celery_config import app	
from airflow.models import Variable
from airflow.operators import TriggerDagRunOperator
from airflow.operators.subdag_operator import SubDagOperator
from subdags import sub_dag
from subdags import Alerts_Engine
import itertools
from itertools import izip_longest
import socket
import paramiko
import sys
import time
import re
import random
import logging
import traceback
from airflow.hooks import MySqlHook
#TODO: Commenting 

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
try:
	system_ip = Variable.get("system_ip")
	key_pattern= Variable.get("key_pattern_create")
	redis_hook = RedisHook(redis_conn_id="redis")
	redis_hook_2 = RedisHook(redis_conn_id="redis_2")
	device_slot=Variable.get("device_slot")
	system_ip = Variable.get("system_ip")
	system_username = Variable.get("system_username")
	system_ssh_password = Variable.get("system_ssh_password")
	system_su_password = Variable.get("system_ssh_password")
	
	PARENT_DAG_NAME="DAG_Connector"
	CHILD_DAG_NAME="Formatter"
	CHILD_DAG_NAME_ALERTS = "Alerts_Engine"
	wait_string = "admin@BreezeVIEW>"
except ValueError:
	logging.error("The required variables are not set please check IN AIRFLOW ADMIN page")

main_dag=DAG(dag_id=PARENT_DAG_NAME, default_args=default_args, schedule_interval='*/5 * * * *')


def send_string_and_wait(command, wait_time, should_print,shell):
	# Send the su command
	shell.send(command)
	# Wait a bit, if necessary
	time.sleep(wait_time)
	# Flush the receive buffer
	receive_buffer = shell.recv(1024)

def send_string_and_wait_for_string(command, wait_string, should_print,shell):
	# Send the su command
	try:
		shell.send(command)
		time.sleep(.5)
		value = []
		# Create a new receive buffer
		receive_buffer = ""
		while not wait_string in receive_buffer:#TODO: As deque is good for pop and push ,thinking to replace the DS to deques but list are good for slicing so will do openerations first on list then change DS
			# Flush the receive buf
			#receive_buffer += shell.recv(1024)
			receive_buffer = shell.recv(1024)
			#time.sleep(.5)
			value.append(receive_buffer)
			shell.send("\n")	 #/n is required to get data from the BV server	
		return value
	except Exception:
		logging.error("Error while getting data from server",exc_info=1)

def connect_and_extract(**kwargs):
	#create the SSH from SSHParmikoHook
	try:
		hook = SSHParmikoHook()	
		x = hook.get_client()
		shell = hook.getShell(x)
		logging.info("Successfully created shell fort BV")
	except Exception:
		logging.error("Exception:",exc_info=1)
		return 'Notify'
	
	send_string_and_wait("sudo su  lteadmin \n", 1, True,shell)
	# Send the client's su password followed by a newline
	send_string_and_wait(system_su_password + "\n", 1, True,shell)	
	send_string_and_wait("ncs_cli -u admin \n", 1, True,shell)
	service_command = Variable.get("cpe_command_get_snapshot")
	snapshot_start=Variable.get("cpe_command_start_snapshot")
	# Send the install command followed by a newline and wait for the done string
	try:
		value_raw = send_string_and_wait_for_string(service_command+ "\n", wait_string, True,shell)
	except Exception:
		logging.error("Error while getting data from server",exc_info=1)
		return 'Notify'
	try:		
		if 'No entries found' not in str(value_raw):
			pattern = re.compile(r'\x1b\[((?:[0-9]*;?)+)([A-Za-z])|([-]*More--[ ]*\[[0-9]*\])')
			out = []
			first_device = ""
			redis_hook.set("cpe_raw_value",str(value_raw))
			logging.debug("Raw Data : "+str(value_raw))
			devices = ""
		    	for entry in value_raw:
				filtered_sting = pattern.sub('',entry)
				filtered_sting=filtered_sting.replace('\r\n',',') # first replace then strip so as to prevent double line output
				filtered_sting=filtered_sting.strip('\r\n')
				devices = devices+filtered_sting
			out = devices.split(',')
			del out[0] # remove 1st element
			del out[-2:]	#remove last 2 elements
			processed_data = list(filter(None,out))		
			#rpush to redis as this is a list
			redis_hook.rpush(key_pattern+str(out[2].split('   ')[1]),str(processed_data))	
			list_of_data = [processed_data[i:i+31] for i in range(0,len(processed_data),31)]
			kwargs['ti'].xcom_push(key='processed_data', value=list_of_data)
			logging.debug("Minimally Processed Data : "+str(list_of_data))
			logging.info("Data processed minimally.")
			#TODO: Add the below to previous send command 
			try:
				response = send_string_and_wait_for_string(snapshot_start+ "\n", wait_string, True,shell)
				logging.debug(response)
				if 'Collection started' in str(response):
					logging.info("Sent command to start the snaphot while leaving the server")
				else:
					logging.info("Sent command to start the snaphot while leaving the server,but snapshot not started")
			except Exception:
				logging.error("Error while getting data from server",exc_info=1)
			shell.close()
			return 'distributeData'
		
		else:
			logging.info("No Entries found in server from the server.")
			return 'Notify'
	except Exception:
		
		logging.error("Exception:",exc_info=1)
		return 'Notify'

def check_reachablity():
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	try:
    		s.connect((Variable.get("system_ip"),22))
		logging.info("BV is reachable om port 22")
		s.close()
		return 'ExtractData'
	except socket.error as e:
    		print "Error on connect: %s" % e
		logging.warning("BV is Not reachable on port 22.")
		return 'Notify'
	
	

def distribute_data(**kwargs):
	try:
		service_data_device_wise = kwargs['ti'].xcom_pull(key='processed_data',task_ids="ExtractData")
	except NameError:
		logging.error("Name Error please see the passing of args is done")
		return 1
	except Exception:
		logging.error("Exception:",exc_info = 1)
		return 2
	
	#service_data_device_wise=[['cpe-view cpes kpi-snapshot cpe-kpi 6CADEF CPE8000 TLR41DFF1C18', ' collection-date 2017-03-19T07:10:07.5+00:00', ' collection-id 2a7a6984-f30e-449a-a1df-a75abd20449e', ' model CPE8000', ' IMSI 001010000007835', ' ip-wan ""', ' up-time 185217', ' ue-status offline', ' serving-enb 1', ' physical-cell-id 16', ' cell-id 0', ' tx-power 7.60', ' RSRP0 -95.00', ' RSRP1 -100.00', ' SINR0 27.00', ' SINR1 24.00', ' MCC 0', ' MNC 0', ' RSRQ -6.00', ' CINR0 0', ' CINR1 0', ' bandwidth 5000', ' tx-data-rate 0', ' rx-data-rate 0', ' DlBLER 0', ' UlBLER 0', ' DlMCS 0', ' UlMCS 0', ' UlRSSI 0', ' UlCINR 0', ' Distance 0'], ['cpe-view cpes kpi-snapshot cpe-kpi 6CADEF CPE8000 TLR41DFF1CB1', ' collection-date 2017-03-19T07:10:06.456+00:00', ' collection-id 2a7a6984-f30e-449a-a1df-a75abd20449e', ' model CPE8000', ' IMSI 001010000007985', ' ip-wan 10.133.18.118', ' up-time 331928', ' ue-status online', ' serving-enb 2', ' physical-cell-id 2', ' cell-id 0', ' tx-power -12.10', ' RSRP0 -69.00', ' RSRP1 -70.00', ' SINR0 33.00', ' SINR1 32.00', ' MCC 0', ' MNC 0', ' RSRQ -7.00', ' CINR0 0', ' CINR1 0', ' bandwidth 5000', ' tx-data-rate 0', ' rx-data-rate 0', ' DlBLER 0', ' UlBLER 0', ' DlMCS 0', ' UlMCS 0', ' UlRSSI 0', ' UlCINR 0', ' Distance 0'], ['cpe-view cpes kpi-snapshot cpe-kpi 6CADEF CPE8000 TLR41DFF1CB1', ' collection-date 2017-03-19T07:10:06.456+00:00', ' collection-id 2a7a6984-f30e-449a-a1df-a75abd20449e', ' model CPE8000', ' IMSI 001010000007985', ' ip-wan 10.133.18.118', ' up-time 331928', ' ue-status online', ' serving-enb 2', ' physical-cell-id 2', ' cell-id 0', ' tx-power -12.10', ' RSRP0 -69.00', ' RSRP1 -70.00', ' SINR0 33.00', ' SINR1 32.00', ' MCC 0', ' MNC 0', ' RSRQ -7.00', ' CINR0 0', ' CINR1 0', ' bandwidth 5000', ' tx-data-rate 0', ' rx-data-rate 0', ' DlBLER 0', ' UlBLER 0', ' DlMCS 0', ' UlMCS 0', ' UlRSSI 0', ' UlCINR 0', ' Distance 0'], ['cpe-view cpes kpi-snapshot cpe-kpi 6CADEF CPE8000 TLR41DFF1CB1', ' collection-date 2017-03-19T07:10:06.456+00:00', ' collection-id 2a7a6984-f30e-449a-a1df-a75abd20449e', ' model CPE8000', ' IMSI 001010000007985', ' ip-wan 10.133.18.118', ' up-time 331928', ' ue-status online', ' serving-enb 2', ' physical-cell-id 2', ' cell-id 0', ' tx-power -12.10', ' RSRP0 -69.00', ' RSRP1 -70.00', ' SINR0 33.00', ' SINR1 32.00', ' MCC 0', ' MNC 0', ' RSRQ -7.00', ' CINR0 0', ' CINR1 0', ' bandwidth 5000', ' tx-data-rate 0', ' rx-data-rate 0', ' DlBLER 0', ' UlBLER 0', ' DlMCS 0', ' UlMCS 0', ' UlRSSI 0', ' UlCINR 0', ' Distance 0'], ['cpe-view cpes kpi-snapshot cpe-kpi 6CADEF CPE8000 TLR41DFF1CB1', ' collection-date 2017-03-19T07:10:06.456+00:00', ' collection-id 2a7a6984-f30e-449a-a1df-a75abd20449e', ' model CPE8000', ' IMSI 001010000007985', ' ip-wan 10.133.18.118', ' up-time 331928', ' ue-status online', ' serving-enb 2', ' physical-cell-id 2', ' cell-id 0', ' tx-power -12.10', ' RSRP0 -69.00', ' RSRP1 -70.00', ' SINR0 33.00', ' SINR1 32.00', ' MCC 0', ' MNC 0', ' RSRQ -7.00', ' CINR0 0', ' CINR1 0', ' bandwidth 5000', ' tx-data-rate 0', ' rx-data-rate 0', ' DlBLER 0', ' UlBLER 0', ' DlMCS 0', ' UlMCS 0', ' UlRSSI 0', ' UlCINR 0', ' Distance 0'], ['cpe-view cpes kpi-snapshot cpe-kpi 6CADEF CPE8000 TLR41DFF1CB1', ' collection-date 2017-03-19T07:10:06.456+00:00', ' collection-id 2a7a6984-f30e-449a-a1df-a75abd20449e', ' model CPE8000', ' IMSI 001010000007985', ' ip-wan 10.133.18.118', ' up-time 331928', ' ue-status online', ' serving-enb 2', ' physical-cell-id 2', ' cell-id 0', ' tx-power -12.10', ' RSRP0 -69.00', ' RSRP1 -70.00', ' SINR0 33.00', ' SINR1 32.00', ' MCC 0', ' MNC 0', ' RSRQ -7.00', ' CINR0 0', ' CINR1 0', ' bandwidth 5000', ' tx-data-rate 0', ' rx-data-rate 0', ' DlBLER 0', ' UlBLER 0', ' DlMCS 0', ' UlMCS 0', ' UlRSSI 0', ' UlCINR 0', ' Distance 0'], ['cpe-view cpes kpi-snapshot cpe-kpi 6CADEF CPE8000 TLR41DFF1CB1', ' collection-date 2017-03-19T07:10:06.456+00:00', ' collection-id 2a7a6984-f30e-449a-a1df-a75abd20449e', ' model CPE8000', ' IMSI 001010000007985', ' ip-wan 10.133.18.118', ' up-time 331928', ' ue-status online', ' serving-enb 2', ' physical-cell-id 2', ' cell-id 0', ' tx-power -12.10', ' RSRP0 -69.00', ' RSRP1 -70.00', ' SINR0 33.00', ' SINR1 32.00', ' MCC 0', ' MNC 0', ' RSRQ -7.00', ' CINR0 0', ' CINR1 0', ' bandwidth 5000', ' tx-data-rate 0', ' rx-data-rate 0', ' DlBLER 0', ' UlBLER 0', ' DlMCS 0', ' UlMCS 0', ' UlRSSI 0', ' UlCINR 0', ' Distance 0']]

	#taking temp value
	#amazing itertools to zip the data in groups #####
	try:
		group_iter = [iter(service_data_device_wise)]*int(device_slot)
		device_slot_data = list(([e for e in t if e !=None] for t in itertools.izip_longest(*group_iter)))
		
	except Exception:
		logging.error("Exception:",exc_info = 1)
		return 3

	if len(device_slot_data) > 0:
		Variable.set("slot_created",len(device_slot_data))
		slot_number = 0
		for slot in device_slot_data:
			print slot
			redis_hook.rpush("cpe_slot_"+str(slot_number),slot) #TODO: create Queue here
			slot_number = slot_number + 1
	else:
		logging.info("The slot created had no devide data in it which is not correct")	
			
def push_combined_data_to_db(**kwargs):

	device_all_data_dict = redis_hook.rget("combined_slot_dict")
	mysql_hook = MySqlHook("nocout_mysql")
	f = open('/home/vipul/myfile.txt', 'w')
	f.write(str(device_all_data_dict))
	f.close()
	conn= mysql_hook.get_conn()
	for device_data_dict in device_all_data_dict:
		eval(device_data_dict)
 		#enter mysql push to DB code here
	


		
def notify():
	print ("Notifying of failure and completing the DAG")

############
#This function gets the static data from the redis connection for use in thresholding and severity calculations
############
def get_required_static_data(**kwargs):
	print ("Get Data from DB")
	service_threshold={} #container to get all services threshoolds
	cpe_services=[service.lower() for service in eval(Variable.get("cpe_services_list"))] # convert everything to lower]
	logging.info(cpe_services)
	for service in cpe_services:
		war_key  = service + ':war'
        	crit_key  = service + ':crit'
		service_threshold[crit_key] = redis_hook_2.get(crit_key)
		service_threshold[war_key] = redis_hook_2.get(war_key)
		logging.info(war_key)	
	try:
		Variable.set("cpe_services_list_thresholds",str(service_threshold))
	except Exception:
		logging.error("Unable to set the thresholds")
		

get_static_data = PythonOperator(
    task_id="getThresholds_severity",
    provide_context=True,
    python_callable=get_required_static_data,
    params={},
    dag=main_dag)
	
extract_data = BranchPythonOperator(
    task_id="ExtractData",
    provide_context=True,
    python_callable=connect_and_extract,
    params={"redis_hook":redis_hook},
    dag=main_dag)

check_reachablity = BranchPythonOperator(
    task_id='ConnectToBVSecurely',
    python_callable=check_reachablity,
    provide_context=False,
    dag=main_dag)

distribute_data = PythonOperator(
    task_id="distributeData",
    provide_context=True,
    python_callable=distribute_data,
    params={"redis_hook":redis_hook},
    dag=main_dag)

formater_data = SubDagOperator(
    subdag=sub_dag(PARENT_DAG_NAME, CHILD_DAG_NAME, datetime(2017, 2, 24),"@once",int(Variable.get("slot_created"))),
    task_id=CHILD_DAG_NAME,
    dag=main_dag
    #provide_context=True
	)
push_data = PythonOperator(
    task_id="PushCombinedDataToDB",
    provide_context=True,
    python_callable=push_combined_data_to_db,
    params={"redis_hook":redis_hook},
    dag=main_dag)

complete= DummyOperator(
    task_id="Completed",
    dag=main_dag)

calculate_alerts = SubDagOperator(
    subdag=Alerts_Engine(PARENT_DAG_NAME, CHILD_DAG_NAME_ALERTS, datetime(2017, 2, 24),"@once",int(Variable.get("slot_created"))),
    task_id=CHILD_DAG_NAME_ALERTS,
    dag=main_dag,
    #provide_context=True
	)

notify = PythonOperator(
    task_id="Notify",
    provide_context=False,
    python_callable=notify,
    dag=main_dag)

check_reachablity >> extract_data >> distribute_data >> formater_data >> calculate_alerts >> complete
get_static_data >> formater_data
check_reachablity >> notify >> complete
extract_data >> notify 
formater_data >> push_data

