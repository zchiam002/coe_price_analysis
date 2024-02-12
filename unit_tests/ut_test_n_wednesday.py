##To test the creation of tables in postgres 
import os
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '')) + '//'
import sys
sys.path.append(root_dir)
data_dir = root_dir + 'unit_tests//input_data//'
save_dir = root_dir + 'unit_tests//output_data//'
import json 

from ancillaries.data_structures.window import Window
from ancillaries.data_structures.generic_value import GenericValue
from ancillaries.date_time_manager import DateTimeManager
from deployment.d_live_pull import DLivePull

def ut_test_n_wednesday ():

    test_name = 'ut_test_n_wednesday'
    result = True

    ##1. Defining the start and the end days of the period
    window = Window({'start': '2024/01/01, 00:00:00', 
                     'end': '2024/02/01, 00:00:00'})
    interval = GenericValue({'value': 1, 'unit': 'day'})
    
    x = DateTimeManager.get_nth_defined_day(window, interval, 'wednesday', 1)

    ##2. Getting teh database credentials 
    local_postgres_connection_details_dir = root_dir + 'data_manager//default_configurations//postgres_connection_details_template_1.json'
    with open (local_postgres_connection_details_dir, 'r') as read_file:
        postgres_connection_details = json.load(read_file)    

    ##Executing the live pull
    live_pull_obj = DLivePull(postgres_connection_details)
    x = live_pull_obj.execute_pull(x.split(', ')[0])

    return test_name, result

#############################################################################################################################################################################
#############################################################################################################################################################################
##Running the script
if __name__ == '__main__':
    test_name, result  = ut_test_n_wednesday ()
