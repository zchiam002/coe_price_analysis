##The class for the generic value
from ancillaries.date_time_manager import DateTimeManager
 
class GenericValue:
    ##The constructor 
    def __init__ (self, generic_value_json):
        self.value = generic_value_json['value']
        self.unit = generic_value_json['unit']
    
    ###########################################################################
    ##Method to convert the value into time_blocks 
    def convert_into_time_block (self, time_block_size_obj):
        time_block = DateTimeManager.time_delta(time_block_size_obj)
        curr_unit = DateTimeManager.time_delta(self)
        
        return int(curr_unit / time_block) 