##To test the creation of tables in postgres 
import os
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '')) + '//'
import sys
sys.path.append(root_dir)
data_dir = root_dir + 'unit_tests//input_data//'
save_dir = root_dir + 'unit_tests//output_data//'

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

    live_pull_obj = DLivePull()
    x = live_pull_obj.execute(x.split(', ')[0])

    print(x)


    return test_name, result

#############################################################################################################################################################################
#############################################################################################################################################################################
##Running the script
if __name__ == '__main__':
    test_name, result  = ut_test_n_wednesday ()
