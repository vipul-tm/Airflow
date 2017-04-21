"""
db_ops.py
==========

Maintains per process database connections and handles 
data manipulations, used in site-wide ETL operations.
[for celery]
"""


from mysql.connector import connect
from redis import Redis
from ConfigParser import ConfigParser
from celery import Task

from start import app


class DatabaseTask(Task):
    abstract = True
    db_conf = app.conf.CNX_FROM_CONF
    # maintains database connections based on sites
    # mysql connections
    my_conn_pool = {}
    # mongo connections
    mo_conn_pool = {}
    # mongo connection object
    #mongo_db = None
    # mysql connection object
    #mysql_db = None

    conf = ConfigParser()
    conf.read(db_conf)


    #@property
    def mysql_cnx(self, key):
    	# mysql connection config
    	my_conf = {
            	'host': self.conf.get(key, 'host'),
            	'port': int(self.conf.get(key, 'port')),
            	'user': self.conf.get(key, 'user'),
            	'password': self.conf.get(key, 'password'),
            	'database': self.conf.get(key, 'database'),
            	}
        try_connect = False
        if not (self.my_conn_pool.get(key) and self.my_conn_pool.get(key).is_connected()):
            try_connect = True
        if try_connect:
        	try:
        		self.my_conn_pool[key] = connect(**my_conf)
        	except Exception as exc:
        		print 'Mysql connection problem, retrying...', exc
        		#raise self.retry(max_retries=2, countdown=10, exc=exc)

        return self.my_conn_pool.get(key)



@app.task(base=DatabaseTask, name='nw-mysql-handler', bind=True)
def mysql_insert_handler(self, data_values, site):
	""" mysql insert and also updates last entry into mongodb"""
	
	# TODO :: remove this extra iteration
	for entry in data_values:
		entry.pop('_id', '')
		entry['sys_timestamp'] = entry['sys_timestamp'].strftime('%s')
		entry['check_timestamp'] = entry['check_timestamp'].strftime('%s')

	try:
		# executing the task locally
		mysql_insert.s('performance_performancenetwork', data_values, site).apply()
	except Exception as exc:
		#lcl_cnx.rollback()
		# may be retry
		print 'Mysql insert problem...', exc

	else:
		# timestamp of most latest insert made to mysql
		last_timestamp = latest_entry(site, op='S') 
		# last timestamp for this insert
		last_timestamp_local = float(data_values[-1].get('sys_timestamp'))
		if (last_timestamp and ((last_timestamp + 900) < last_timestamp_local)):
			#print 'last_timestamp %s' % last_timestamp
			#print 'last_timestamp_local %s' % last_timestamp_local
			# mysql is down for more than 30 minutes from now,
			# import data from mongo
			rds_cli = Redis(port=app.conf.REDIS_PORT)
			lock = rds_cli.lock('unique_key', timeout=5*60)
			have_lock = lock.acquire(blocking=False)
			# ensure, only one task is executed at one time
			# TODO :: call the commented task
			if have_lock:
				pass
				#mongo_export_mysql(last_timestamp, last_timestamp_local,
			    #		'network_perf', 'performance_performancenetwork')
		
		# update the `latest_entry` collection
		latest_entry(site, op='I', value=last_timestamp_local)

	# sending a message for task, execute asynchronously
	mysql_update.s('performance_networkstatus', data_values, site
			).apply_async()


@app.task(base=DatabaseTask, name='nw-mysql-update', bind=True)
def mysql_update(self, table, data_values, site):
    """ mysql update"""
	
    #upsert_dict = {'inserts': [], 'updates': []}
    #slct_qry = """
    #         SELECT 1 FROM performance_networkstatus 
    #         WHERE device_name = %(device_name)s AND
    #         service_name = %(service_name)s AND
    #         data_source = %(data_source)s
    #         """
    #updt_qry = "UPDATE %(table)s SET " % {'table': table}
    updt_qry = "INSERT INTO %(table)s" % {'table': table}
    updt_qry += """
		    (
		    	device_name, 
		    	service_name, 
		    	machine_name,
				site_name, 
				ip_address, 
				data_source, 
				severity, 
				current_value,
				min_value, 
				max_value, 
				avg_value, 
				warning_threshold, 
				critical_threshold, 
				sys_timestamp, 
				check_timestamp, 
				age, 
				refer
			) 
			VALUES 
			(
				%(device_name)s, 
				%(service_name)s, 
				%(machine_name)s, 
				%(site_name)s, 
				%(ip_address)s, 
				%(data_source)s, 
				%(severity)s, 
				%(current_value)s, 
				%(min_value)s, 
				%(max_value)s, 
				%(avg_value)s, 
				%(warning_threshold)s, 
				%(critical_threshold)s, 
				%(sys_timestamp)s, 
				%(check_timestamp)s, 
				%(age)s, 
				%(refer)s
			)
			ON DUPLICATE KEY UPDATE
			machine_name = VALUES(machine_name),
			site_name 	 = VALUES(site_name), 
			ip_address 	 = VALUES(ip_address), 	
			severity 	 = VALUES(severity), 
			current_value  = VALUES(current_value),
			min_value 	 = VALUES(min_value), 
			max_value 	 = VALUES(max_value), 
			avg_value 	 = VALUES(avg_value), 
			warning_threshold = VALUES(warning_threshold), 
			critical_threshold = VALUES(critical_threshold), 
			sys_timestamp  = VALUES(sys_timestamp), 
			check_timestamp = VALUES(check_timestamp), 
			age 		 = VALUES(age), 
			refer 		 = VALUES(refer)
			"""
         	#machine_name = %(machine_name)s, site_name = %(site_name)s, 
            #current_value = %(current_value)s, min_value = %(min_value)s, 
            #max_value = %(max_value)s, avg_value = %(avg_value)s, 
            #warning_threshold = %(warning_threshold)s, critical_threshold = 
            #%(critical_threshold)s, sys_timestamp = %(sys_timestamp)s, 
            #check_timestamp = %(check_timestamp)s, ip_address = %(ip_address)s, 
            #severity = %(severity)s, age = %(age)s, refer = %(refer)s
            #WHERE device_name = %(device_name)s AND service_name = 
            #%(service_name)s AND data_source = %(data_source)s
            #"""
          
    try:
    	lcl_cnx = mysql_update.mysql_cnx(site)
    	cur = lcl_cnx.cursor()
    	cur.executemany(updt_qry, data_values)
    	lcl_cnx.commit()
    	cur.close()
    except Exception as exc:
    	# rollback transaction
    	lcl_cnx.rollback()
    	# attempt task retry
    	raise self.retry(args=(table, data_values, site), max_retries=1, countdown=10, 
    			exc=exc)

    #for i in xrange(len(data_values)):
    #	cur.execute(slct_qry, data_values[i])
    #	if cur.fetchone():
    #		upsert_dict['updates'].append(data_values[i])
    #	else:
    #		upsert_dict['inserts'].append(data_values[i])
    print 'Len for status upserts %s\n' % len(data_values)

    #if upsert_dict['updates']:
    #	# make a new cursor instance, to get rid of `unread results` error
    #	cur = lcl_cnx.cursor()
    #	try:
    #		cur.executemany(updt_qry, upsert_dict['updates'])
    #		lcl_cnx.commit()
    #		cur.close()
    #	except Exception as exc:
    #		# TODO :: manage task retries
    #		print 'Problem in network update, rollback...', exc
    #		lcl_cnx.rollback()
    #if upsert_dict['inserts']:
    #	mysql_insert.s('performance_networkstatus', upsert_dict['inserts'], site
    #			).apply_async()


@app.task(base=DatabaseTask, name='nw-mysql-insert', bind=True)
def mysql_insert(self, table, data_values, site):
	""" mysql batch insert"""
	
	# TODO :: custom option for retries
	try:
		fmt_qry = "INSERT INTO %(table)s " % {'table': table}
		fmt_qry += """
		        (device_name, service_name, machine_name,
				site_name, ip_address, data_source, severity, current_value,
				min_value, max_value, avg_value, warning_threshold, 
				critical_threshold, sys_timestamp, check_timestamp, age, refer) 
				VALUES 
				(%(device_name)s, %(service_name)s, %(machine_name)s, %(site_name)s, 
				%(ip_address)s, %(data_source)s, %(severity)s, %(current_value)s, 
				%(min_value)s, %(max_value)s, %(avg_value)s, %(warning_threshold)s, 
				%(critical_threshold)s, %(sys_timestamp)s, %(check_timestamp)s, 
				%(age)s, %(refer)s)
		    	"""
		lcl_cnx = mysql_insert.mysql_cnx(site)
		cur = lcl_cnx.cursor()
		#cur.executemany(qry, map(lambda x: fmt_qry(**x), data_values))
		cur.executemany(fmt_qry, data_values)
		lcl_cnx.commit()
		cur.close()
	except Exception as exc:
		# rollback transaction
		lcl_cnx.rollback()
		print 'Error in mysql_insert task, ', exc


@app.task(base=DatabaseTask, name='get-latest-entry', bind=True)
def latest_entry(self, site, op='S', value=None):
	lcl_cnx = latest_entry.mongo_cnx(site)
	stamp = None
	if op == 'S':
		# select operation
		try:
			stamp = list(lcl_cnx['latest_entry'].find())[0].get('time')
		except: pass
	elif op == 'I':
		# insert operation
		lcl_cnx['latest_entry'].update({'_id': 1}, 
				{'time': value, '_id': 1}, upsert=True)

	return float(stamp) if stamp else stamp


@app.task(base=DatabaseTask, name='mongo-export-mysql', bind=True)
def mongo_export_mysql(self, start_time, end_time, col, table, site):
	""" Export old data which is not in mysql due to its downtime"""
	
	print 'Mongo export mysql called'
	data_values = list(mongo_export_mysql.mongo_cnx(site)[col].find(
			{'local_timestamp': {'$gt': start_time, '$lt': end_time}}))

	# TODO :: data should be sent into batches
	# or send the task into celery chuncks, dont execute the task locally
	mysql_insert.s('performance_performancenetwork', data_values, site).apply_async()

