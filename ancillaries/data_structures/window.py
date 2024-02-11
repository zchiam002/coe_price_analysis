##The main class for the market product window
import copy
import pytz

from ancillaries.date_time_manager import DateTimeManager

class Window:
    ##The constructor 
    def __init__ (self, window_json):
        self.start = DateTimeManager.string_to_object(window_json['start'])
        self.end = DateTimeManager.string_to_object(window_json['end'])
    ###########################################################################
    ##Method to compare with another window
    def compare_window (self, to_compare_window):
        ret_value = True
        if self.start != to_compare_window.start:
            ret_value = False 
        elif self.end != to_compare_window.end:
            ret_value = False
        return ret_value 
    ###########################################################################
    ##Method to convert into a dictionary 
    def to_dict (self):
        ret_dict = {}
        ret_dict['start'] = copy.deepcopy(DateTimeManager.object_to_string(self.start))
        if str(self.start.tz) == 'UTC':
            ret_dict['start'] = ret_dict['start'].replace(', ', 'T') + 'Z'

        ret_dict['end'] = copy.deepcopy(DateTimeManager.object_to_string(self.end))
        if str(self.end.tz) == 'UTC':
            ret_dict['end'] = ret_dict['end'].replace(', ', 'T') + 'Z'

        return ret_dict