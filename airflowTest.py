from nocout_site_name import *
import socket,json
import time
import imp
import re
from collections import defaultdict
from itertools import groupby
from operator import itemgetter

from datetime import datetime, timedelta

utility_module = imp.load_source('utility_functions', '/omd/sites/%s/nocout/utils/utility_functions.py' % nocout_site_name)
mongo_module = imp.load_source('mongo_functions', '/omd/sites/%s/nocout/utils/mongo_functions.py' % nocout_site_name)
config_module = imp.load_source('configparser', '/omd/sites/%s/nocout/configparser.py' % nocout_site_name)
db_ops_module = imp.load_source('db_ops', '/omd/sites/%s/lib/python/handlers/db_ops.py' % nocout_site_name)

def inventory_perf_data_main():
        """
        inventory_perf_data_main : Main Function for data extraction for inventory services.Function get all configuration from config.ini
        Args: None
        Kwargs: None

        Return : None
        Raises: No Exception
       	"""
    try:
        site="ospf2_slave1"
        query = "GET hosts\nColumns: host_name\nOutputFormat: json\n"
        output = json.loads(utility_module.get_from_socket(site,query))
        print "Output is ---> "+ str(output)
        except:
            print "Finally Done"
            pass

if __name__ == '__main__':
    inventory_perf_data_main()