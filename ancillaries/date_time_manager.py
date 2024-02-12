##Class for managing the date time for the project 
import datetime
import pandas as pd 
import pytz

UTC_TIME_DIFFERENCE = datetime.timedelta(hours = 8)

class DateTimeManager:
    ###########################################################################
    ##Method to get the unique month list 
    def get_unique_month_list (window):

        time_list = DateTimeManager.get_years_months_days(window.start, window.end)

        tracker_str = []
        ret_list = []
        
        for element in time_list:
            curr_year = str(element.year)
            curr_month = str(element.month)
            if len(curr_month) == 1:
                curr_month = '0' + curr_month 
            curr_day = '01' 
            curr_date_str = curr_year + '/' + curr_month + '/' + curr_day
            
            if curr_date_str not in tracker_str:
                tracker_str.append(curr_date_str)
                ret_list.append(DateTimeManager.string_to_object(curr_date_str))

        return ret_list
    
    ###########################################################################
    ##Method to find the nth defined day of the given period 
    def get_nth_defined_day (window, interval, day_string: str, occurence: int):
        day_list = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        assert day_string in day_list 

        time_stamps_str_list = DateTimeManager.time_stamps(window.start, window.end, interval, with_date = True)

        ret_value = None
        count = 0 
        for time_stamp_str in time_stamps_str_list:
            time_stamp_obj = DateTimeManager.string_to_object(time_stamp_str[1])
            curr_day = time_stamp_obj.weekday()
            
            if curr_day == day_list.index(day_string):
                count = count + 1
            
            if count == occurence:
                ret_value = time_stamp_str[1]
                break 

        return ret_value 
    
    ###########################################################################
    ##Method to get the time-stamps, with a specified interval between two time-stamps 
    def time_stamps (from_time, to_time, interval, time_only = False, with_date = False):
        assert to_time >= from_time 
        block_size = DateTimeManager.time_delta(interval)

        ret_list = []
        curr_time = from_time
        fuse = 100000

        while curr_time < to_time:
            if with_date == False:
                ret_list.append(DateTimeManager.object_to_string(curr_time, time_only = time_only))
            else:
                time_stamp = DateTimeManager.object_to_string(curr_time, time_only = time_only)
                date_stamp = DateTimeManager.object_to_string(curr_time, date_only = True)
                ret_list.append([date_stamp, time_stamp])
            curr_time = curr_time + block_size
            
            ##As a safety measure
            fuse = fuse - 1
            if fuse <= 0:
                raise Exception('Fused! Please check implementation... ...')

        return ret_list 
    ###########################################################################
    ##Method to get the time difference between two time stamps 
    def duration (from_time, to_time, target_units = 'sec'):
        duration = to_time - from_time 
        ret_value = duration.total_seconds() 
        
        if target_units == 'sec':
            pass 
        elif target_units == 'min':
            ret_value = ret_value / 60
        elif target_units == 'hr':
            ret_value = ret_value / (60 * 60)
        elif target_units == 'day':
            ret_value = ret_value / (60 * 60 * 24)
        elif target_units == 'week':
            ret_value = ret_value / (60 * 60 * 24 * 7)
        else:
            raise Exception ('Invalid target unit encountered (' + str(target_units) + ')... ... Please review... ...' )             
    
        return ret_value
    ###########################################################################
    ##Method to get time delta objects for intervals with different units 
    def time_delta (generic_value_obj):
        
        if generic_value_obj.unit == 'week':
            ret_value = datetime.timedelta(weeks = generic_value_obj.value)    
        elif generic_value_obj.unit == 'day':
            ret_value = datetime.timedelta(days = generic_value_obj.value)
        elif generic_value_obj.unit == 'hr':
            ret_value = datetime.timedelta(hours = generic_value_obj.value)
        elif generic_value_obj.unit == 'min':
            ret_value = datetime.timedelta(minutes = generic_value_obj.value)
        elif generic_value_obj.unit == 'sec':
            ret_value = datetime.timedelta(seconds = generic_value_obj.value)
        else:
            raise Exception ('Invalid time horizon unit encountered (' + str(generic_value_obj.unit) + ')... ... Please review... ...' )        
        
        return ret_value
    ###########################################################################
    ##Method to get the number of years, months and days from a given date range 
    def get_years_months_days (start_date, end_date):

        dataframe = pd.DataFrame(data = [[start_date], [end_date]], columns = ['time'])
        dataframe.set_index(['time'], inplace = True)
        resampled_df = dataframe.resample('1D').interpolate()
        time_list = list(resampled_df.index)        
    
        return time_list 
    ###########################################################################
    ##To convert string into object 
    def string_to_object (date_time_string):
        ret_value = None
        if type(date_time_string) == str:
            dataframe = pd.DataFrame(data = [[date_time_string]], columns = ['timestamp'])
            datetimes = pd.to_datetime(dataframe['timestamp'])
            ret_value = datetimes[0]
        return ret_value
    
    ###########################################################################
    ##To convert object into string 
    def object_to_string (date_time_object, time_only = False, date_only = False, date_separator = '/'):
        
        ret_value = None 
        
        if date_time_object != None:
            ##Defining date time string format 
            if time_only == True:
                time_stamp_format = '%H:%M:%S'
            elif date_only == True:
                time_stamp_format = '%Y' + date_separator + '%m' + date_separator + '%d'
            else:
                time_stamp_format = '%Y' + date_separator + '%m' + date_separator + '%d, %H:%M:%S'
            
            ret_value = date_time_object.strftime(time_stamp_format)
        
        return ret_value
    ###########################################################################
    ##To convert timezone-aware object to timezone-naive object
    def tz_naive_to_aware (date_time_object):
        ret_value = date_time_object.replace(tzinfo=pytz.UTC)

        return ret_value
    ###########################################################################
    ##To convert local time (timezone-naive) to UTC time (timezone-aware object)
    def local_to_utc (date_time_object):
        ret_value = date_time_object
        ret_value -= UTC_TIME_DIFFERENCE
        ret_value = ret_value.replace(tzinfo = pytz.UTC)
        
        return ret_value


        