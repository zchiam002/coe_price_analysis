##The main class for pulling live data 
import time
import requests 
from bs4 import BeautifulSoup
import holidays
import datetime
import hashlib
import pandas as pd 

from data_manager.data_manager import DataManager
from ancillaries.data_structures.window import Window
from ancillaries.data_structures.generic_value import GenericValue
from ancillaries.date_time_manager import DateTimeManager
from deployment.d_helper.dh_get_database_connector import DHDatabaseConnector

class DLivePull:
    ##The constructor 
    def __init__ (self, connection_details_json):
        self.interval = GenericValue({'value': 1, 'unit': 'day'})
        self.holiday_checker = holidays.SG()

        self.connection_details_json = connection_details_json
        self.connection_details_json['filename'] = 'table_coe_premium.json'
        self.database_connector = DHDatabaseConnector(connection_details_json)

    ###########################################################################
    ##Method to execute live pull
    def execute (self, time_difference = GenericValue({'value': 8, 'unit': 'hr'})):
        
        x = True
        while x == True:
            curr_time = datetime.datetime.utcnow()
            curr_time_str = DateTimeManager.object_to_string(curr_time).replace(', ', 'T') + 'Z'
            curr_time = DateTimeManager.string_to_object(curr_time_str)

            print('Attempting pull for ', curr_time)

            curr_time_year = curr_time.year 
            curr_time_month = curr_time.month

            curr_time_year_str = str(curr_time_year)
            curr_time_month_str = str(curr_time_month)
            if len(curr_time_month_str) == 1:
                curr_time_month_str = '0' + curr_time_month_str
            
            curr_day_str = curr_time_year_str + '/' + curr_time_month_str + '01T00:00:00Z'
            curr_day = DateTimeManager.string_to_object(curr_day_str)

            ##Get the current month 
            curr_year = curr_time.year 
            curr_month = curr_time.month 

            next_month = curr_month + 1
            if next_month > 12:
                next_month = 1
                next_year = curr_year + 1
            else:
                next_year = curr_year + 1

            curr_year_str = str(curr_year)
            next_year_str = str(next_year)

            curr_month_str = str(curr_month)
            if len(curr_month_str) == 1:
                curr_month_str = '0' + curr_month_str

            next_month_str = str(next_month)
            if len(next_month_str) == 1:
                next_month_str = '0' + next_month_str
            
            curr_window = Window({'start': curr_year_str + '/' + curr_month_str + '/01, 00:00:00',
                                'end': next_year_str + '/' + next_month_str + '/01, 00:00:00'})

            first_wed_date = DateTimeManager.get_nth_defined_day(curr_window, self.interval, 'wednesday', 1)
            third_wed_date = DateTimeManager.get_nth_defined_day(curr_window, self.interval, 'wednesday', 3)

            ##Next we need to check if it is a public holiday or not 
            fuse = 7
            continue_1 = False 
            if self.holiday_checker.get(first_wed_date) != None:
                continue_1 = True 

            while continue_1 == True:
                first_wed_date = DateTimeManager.object_to_string(DateTimeManager.string_to_object(first_wed_date) + DateTimeManager.time_delta(self.interval))

                continue_1 = False
                if self.holiday_checker.get(first_wed_date) != None:
                    continue_1 = True 
                
                if DateTimeManager.string_to_object(first_wed_date).weekday() in [5, 6]:
                    continue_1 = True

                fuse = fuse + 1
                if fuse <= 0:
                    raise Exception ('Fuse-limit reached! Please review implementation... ...')

            fuse = 7
            continue_1 = False 
            if self.holiday_checker.get(third_wed_date) != None:
                continue_1 = True 

            while continue_1 == True:
                third_wed_date = DateTimeManager.object_to_string(DateTimeManager.string_to_object(third_wed_date) + DateTimeManager.time_delta(self.interval))

                continue_1 = False
                if self.holiday_checker.get(third_wed_date) != None:
                    continue_1 = True 
                
                if DateTimeManager.string_to_object(third_wed_date).weekday() in [5, 6]:
                    continue_1 = True

                fuse = fuse + 1
                if fuse <= 0:
                    raise Exception ('Fuse-limit reached! Please review implementation... ...')
        
            first_wed_date_obj = DateTimeManager.string_to_object(first_wed_date.replace(', ', 'T') + 'Z') - DateTimeManager.time_delta(time_difference)
            if first_wed_date_obj == curr_day:
                self.execute_pull(first_wed_date)

            third_wed_date_obj = DateTimeManager.string_to_object(third_wed_date.replace(', ', 'T') + 'Z') - DateTimeManager.time_delta(time_difference)
            if third_wed_date_obj == curr_day:
                self.execute_pull(third_wed_date)

            time.sleep(86400)

        return 
    
    ###########################################################################
    ##Method to execute the backfill
    def backfill (self, window: Window, time_difference = GenericValue({'value': 8, 'unit': 'hr'})):
        
        curr_time = datetime.datetime.utcnow()
        curr_time_str = DateTimeManager.object_to_string(curr_time).replace(', ', 'T') + 'Z'
        curr_time = DateTimeManager.string_to_object(curr_time_str)

        ##First we need get the unique months within the window 
        month_list = DateTimeManager.get_unique_month_list(window)

        ##Executing the pull
        for element_idx in range(0, len(month_list) - 1):

            print(month_list[element_idx])

            ##Form the window 
            window_start_str = DateTimeManager.object_to_string(month_list[element_idx])
            window_end_str = DateTimeManager.object_to_string(month_list[element_idx + 1])

            curr_window = Window({'start': window_start_str, 
                                  'end': window_end_str})
            
            first_wed_date = DateTimeManager.get_nth_defined_day(curr_window, self.interval, 'wednesday', 1)
            third_wed_date = DateTimeManager.get_nth_defined_day(curr_window, self.interval, 'wednesday', 3)

            ##Next we need to check if it is a public holiday or not 
            fuse = 7
            continue_1 = False 
            if self.holiday_checker.get(first_wed_date) != None:
                continue_1 = True 
    
            while continue_1 == True:
                first_wed_date = DateTimeManager.object_to_string(DateTimeManager.string_to_object(first_wed_date) + DateTimeManager.time_delta(self.interval))

                continue_1 = False
                if self.holiday_checker.get(first_wed_date) != None:
                    continue_1 = True 
                
                if DateTimeManager.string_to_object(first_wed_date).weekday() in [5, 6]:
                    continue_1 = True

                fuse = fuse + 1
                if fuse <= 0:
                    raise Exception ('Fuse-limit reached! Please review implementation... ...')

            fuse = 7
            continue_1 = False 
            if self.holiday_checker.get(third_wed_date) != None:
                continue_1 = True 
    
            while continue_1 == True:
                third_wed_date = DateTimeManager.object_to_string(DateTimeManager.string_to_object(third_wed_date) + DateTimeManager.time_delta(self.interval))

                continue_1 = False
                if self.holiday_checker.get(third_wed_date) != None:
                    continue_1 = True 
                
                if DateTimeManager.string_to_object(third_wed_date).weekday() in [5, 6]:
                    continue_1 = True

                fuse = fuse + 1
                if fuse <= 0:
                    raise Exception ('Fuse-limit reached! Please review implementation... ...')
                
            first_wed_date_obj = DateTimeManager.string_to_object(first_wed_date.replace(', ', 'T') + 'Z') - DateTimeManager.time_delta(time_difference)
            if first_wed_date_obj < curr_time:
                self.execute_pull(first_wed_date)

            third_wed_date_obj = DateTimeManager.string_to_object(third_wed_date.replace(', ', 'T') + 'Z') - DateTimeManager.time_delta(time_difference)
            if third_wed_date_obj < curr_time:
                self.execute_pull(third_wed_date)

        return 
        
    ###########################################################################
    ##Method to execute the live pull 
    def execute_pull (self, date_stamp_str, time_difference = GenericValue({'value': 8, 'unit': 'hr'})):
        ##Assempling the query 
        date_stamp_str = date_stamp_str.replace('/', '-')
        
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        url_base = 'https://www.motorist.sg/coe-result/' + date_stamp_str
        r = requests.get(url_base, headers=headers, timeout = 5)
        
        soup = BeautifulSoup(r.text, 'html.parser')

        table = soup.find('table', {'class': 'table table-borderless'})

        description_rows = [1, 3, 5, 7, 9]
        quota_premium_rows = [10, 11, 12, 13, 14]
        delta_rows = [15, 16, 17, 18, 19]
        pqp_rows = [20, 21, 22, 23, 24]
        quota_rows = [25, 26, 27, 28, 29]
        bids_received_rows = [30, 31, 32, 33, 34]

        category_data = ['A', 'B', 'C', 'D', 'E']
        description_data = []
        quota_premium_data = []
        delta_data = []
        pqp_data = []
        quota_data = []
        bids_received_data = []

        count = 0

        for row in table.find_all('p'):
            curr_text = row.text 
            if count in description_rows:
                description_data.append(curr_text)
            elif count in quota_premium_rows:
                to_append = curr_text[1:]
                to_append = float(to_append.replace(',', ''))
                quota_premium_data.append(to_append)
            elif count in delta_rows:
                to_append = curr_text[2:]
                try:
                    to_append = float(to_append.replace(',', ''))
                except:
                    to_append = -99999.0
                delta_data.append(to_append)
            elif count in pqp_rows:
                pqp_data.append(curr_text)
            elif count in quota_rows:
                to_append = float(curr_text.replace(',', ''))
                quota_data.append(to_append)
            elif count in bids_received_rows:
                to_append = float(curr_text.replace(',', ''))
                bids_received_data.append(to_append)

            count = count + 1  

        ingestion_time_utc = datetime.datetime.utcnow()
        uuid = 1

        column_names = ['date', 'category', 'description', 'quota_premium', 'delta', 'pqp', 'quota', 'bids_received']

        temp_data = []
        for idx in range (0, len(category_data)):
            date_obj = DateTimeManager.string_to_object(date_stamp_str) - DateTimeManager.time_delta(time_difference)
            date_str = DateTimeManager.object_to_string(date_obj)
            date_utc_str = date_str.replace(', ', 'T') + 'Z'
            date_utc_obj = DateTimeManager.string_to_object(date_utc_str) 

            temp_data.append([date_utc_obj, category_data[idx], description_data[idx], quota_premium_data[idx],
                              delta_data[idx], pqp_data[idx], quota_data[idx], bids_received_data[idx]])
            
        
        final_df = pd.DataFrame(data = temp_data, columns = column_names)

        ##Now we need to ingest into the database 
        self.__ingest_data(final_df)

        return final_df

    ######################################################################
    ##INTERNAL: Method to write the pulled data into the database
    def __ingest_data (self, data_df):

        ##Get the incoming time 
        outgoing_time = datetime.datetime.utcnow()
        outgoing_time_str = DateTimeManager.object_to_string(outgoing_time, date_separator = '-').replace(', ', 'T') + 'Z'

        for row in data_df.index:
            curr_hash_str_to_hash = outgoing_time_str
            curr_row_data = [outgoing_time]
            
            for column in data_df.columns:
                curr_hash_str_to_hash = curr_hash_str_to_hash + '_' + str(data_df[column][row])
                curr_row_data.append(data_df[column][row])

            ##Creating a uuid 
            hashed_str = hashlib.md5(bytes(curr_hash_str_to_hash, 'utf-8')).hexdigest()

            ##Ingesting into the database
            data_to_ingest = [[hashed_str] + curr_row_data]

            table_name = 'coe_premium'
            connection = self.database_connector.connection_details[table_name]['connection']
            cursor = self.database_connector.connection_details[table_name]['cursor']
            table_details = self.database_connector.get_table_details(table_name)

            DataManager.Postgresql.ingest_data(connection, cursor, table_name, table_details['column_details'], data_to_ingest)

        return 