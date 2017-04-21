from airflow.models import Variable
from airflow.operators import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook

SQLhook=MySqlHook(mysql_conn_id='mysql_uat')

#############################################HELPERS################################################
def dict_rows(cur):
    desc = cur.description
    return [
        dict(zip([col[0] for col in desc], row))
        for row in cur.fetchall()
    ]

def execute_query(query):
	conn = SQLhook.get_conn()
	cursor = conn.cursor()
	cursor.execute(query)
	data =  dict_rows(cursor)
	cursor.close()
	return data

def createDict(data):
	#TODOL There are 3 levels of critality handle all those(service_critical,critical,dtype_critical)
	rules = {}
	ping_rule_dict = {}
	for device in data:
		service_name = device.get('service')
		device_type = device.get('devicetype')
		if service_name:
			name =  str(service_name)
			rules[name] = {}
		if device.get('critical'):
			rules[name]={"Severity1":["critical",{'name': str(name)+"_critical", 'operator': 'greater_than', 'value': device.get('critical') or device.get('dtype_ds_critical')}]}
		else:
			rules[name]={"Severity1":["critical",{'name': str(name)+"_critical", 'operator': 'greater_than', 'value': 'unknown'}]}
		if device.get('warning'):
			rules[name].update({"Severity2":["warning",{'name': str(name)+"_warning", 'operator': 'greater_than', 'value': device.get('warning') or device.get('dtype_ds_warning')}]})
		else:
			rules[name].update({"Severity2":["warning",{'name': str(name)+"_warning", 'operator': 'greater_than', 'value': 'unknown'}]})
		if device_type not in ping_rule_dict:
			if device.get('ping_pl_critical') and device.get('ping_pl_warning') and device.get('ping_rta_critical') and device.get('ping_rta_warning'):
				ping_rule_dict[device_type] = {
				'ping_pl_critical' : device.get('ping_pl_critical'),
				'ping_pl_warning': 	 device.get('ping_pl_warning') ,
				'ping_rta_critical': device.get('ping_rta_critical'),
				'ping_rta_warning':  device.get('ping_rta_warning')
				}
	for device_type in  ping_rule_dict:
		if ping_rule_dict.get(device_type).get('ping_pl_critical'):
			rules[device_type+"_pl"] = {"Severity1":["critical",{'name': device_type+"_pl_critical", 'operator': 'greater_than', 'value': int(ping_rule_dict.get(device_type).get('ping_pl_critical')) or 'unknown'}]}
		if ping_rule_dict.get(device_type).get('ping_pl_warning'):
			rules[device_type+"_pl"].update({"Severity2":["warning",{'name': device_type+"_pl_warning", 'operator': 'less_than', 'value': int(ping_rule_dict.get(device_type).get('ping_pl_warning')) or 'unknown'}]})
		if ping_rule_dict.get(device_type).get('ping_rta_critical'):
			rules[device_type+"_rta"] = {"Severity1":["critical",{'name': device_type+"_rta_critical", 'operator': 'greater_than', 'value': int(ping_rule_dict.get(device_type).get('ping_rta_critical')) or 'unknown'}]}
		if ping_rule_dict.get(device_type).get('ping_rta_warning'):
			rules[device_type+"_rta"].update({"Severity2":["warning",{'name': device_type+"_rta_warning", 'operator': 'less_than', 'value': int(ping_rule_dict.get(device_type).get('ping_rta_warning')) or 'unknown'}]})
		
	return rules

###############################################ETL FUNCTIONS########################################################################################
def get_required_static_data():
	print ("Extracting Data")
	service_threshold_query = Variable.get('q_get_thresholds')
	print (service_threshold_query)
	data = execute_query(service_threshold_query)
	print data[0]
	rules_dict = createDict(data)
	Variable.set("rules",str(rules_dict))

def init_etl():
	print("TODO : Check All vars and Airflow ETL Environment here")





