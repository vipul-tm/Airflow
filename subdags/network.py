from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators import PythonOperator
from airflow.hooks import RedisHook
from airflow.models import Variable
import logging
import itertools

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

redis_hook_4 = RedisHook(redis_conn_id="redis_hook_4") #number specifies the DB in Use

def network_etl(parent_dag_name, child_dag_name, start_date, schedule_interval):
	config = eval(Variable.get('system_config'))
	dag_subdag = DAG(
    		dag_id="%s.%s" % (parent_dag_name, child_dag_name),
    		schedule_interval=schedule_interval,
    		start_date=start_date,
  		)
	#TODO: Create hook for using socket with Pool
	def get_from_socket(site_name,query,socket_ip,socket_port):
		"""
		Function_name : get_from_socket (collect the query data from the socket)

		Args: site_name (poller on which monitoring data is to be collected)

		Kwargs: query (query for which data to be collectes from nagios.)

		Return : None

		raise
		     Exception: SyntaxError,socket error
	    	"""
		#socket_path = "/omd/sites/%s/tmp/run/live" % site_name
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		#s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
		machine = site_name[:-8]
		s.connect((socket_ip, socket_port))
		#s.connect(socket_path)
		s.send(query)
		s.shutdown(socket.SHUT_WR)
		output = ''
		wait_string= ''
		while True:
			try:
				out = s.recv(100000000)
		     	except socket.timeout,e:
				err=e.args[0]
				print 'socket timeout ..Exiting'
				if err == 'timed out':
					sys.exit(1) 
		
		     	if not len(out):
				break;
		     	output += out

		return output
	
	def extract_and_distribute(**kwargs):
		try:
			network_query = Variable.get('network_query')
			device_slot = Variable.get("device_slot_network")
		except Exception:
			logging.info("Unable to fetch Network Query Failing Task")
			return 1

		task_site = kwargs.get('task_instance_key_str').split('_')[4:7]
		site_name = "_".join(task_site)
		site_ip = kwargs.get('params').get("ip")
		site_port = kwargs.get('params').get("port")
		logging.info("Extracting data for site"+str(site_ip)+"port "+str(site_port))
		network_data = []
		#network_data = eval(get_from_socket(site_name, network_query,site_ip,site_port))
		network_data = [[u'1',u'10.171.132.2',0,1491822300,1491622173,u'rta=1.151ms;50.000;60.00;0;pl=0%;10;20;; rtmax=1.389ms;;;; rtmin=1.035ms;;;;']] * 40 
		#TODO: 		
		group_iter = [iter(network_data)]*int(device_slot)
		device_slot_data = list(([e for e in t if e !=None] for t in itertools.izip_longest(*group_iter)))
		print len(device_slot_data)
		i=1;		
		for slot in device_slot_data:	
			redis_hook_4.rpush("nw_"+site_name+"_slot_"+str(i),slot)
			i+=1


	for machine in config :
		for site in machine.get('sites'):
			PythonOperator(
	   		task_id="Network_extract_%s"%(site.get('name')),
	  		provide_context=True,
	 		python_callable=extract_and_distribute,
			params={"ip":machine.get('ip'),"port":site.get('port')},
			dag=dag_subdag
			)
	return 	dag_subdag

