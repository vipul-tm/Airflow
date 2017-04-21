###############Utility functions for format################

import re
from airflow.models import Variable
def get_threshold(perf_data):
    """
    Function_name : get_threshold (function for parsing the performance data and storing in the datastructure)

    Args: perf_data performance_data extracted from rrdtool

    Kwargs: None
    return:
           threshold_values (data strucutre containing the performance_data for all data sources)
    Exception:
           None
    """

    threshold_values= {}
    perf_data_list = filter(None,perf_data.split(";"))

    for element in perf_data_list:
         if "=" in element:
            splitted_val = element.split("=")
            splitted_val[1] = splitted_val[1].strip("ms") if "ms" in splitted_val[1] else splitted_val[1]
            threshold_values[splitted_val[0].strip()] = {
            'cur': splitted_val[1]
            
            }

    return threshold_values

def get_device_type_from_name(hostname):
    hostmk = Variable.get("hostmk.dict")
    hostmk = eval(hostmk)
    try:
        device_type = hostmk.get(hostname)
        return device_type
    except:
        return 'unknown'