##The main class for pulling live data 
import requests 
from bs4 import BeautifulSoup
import holidays
import datetime
import pandas as pd 

from ancillaries.data_structures.window import Window
from ancillaries.data_structures.generic_value import GenericValue
from ancillaries.date_time_manager import DateTimeManager

class DLivePull:
    ##The constructor 
    def __init__ (self):
        self.interval = GenericValue({'value': 1, 'unit': 'day'})
        self.holiday_checker = holidays.SG()
    ###########################################################################
    ##Method to execute the live pull 
    def execute (self, date_stamp_str):
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
                to_append = float(to_append.replace(',', ''))
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
            temp_data.append([date_stamp_str, category_data[idx], description_data[idx], quota_premium_data[idx],
                              delta_data[idx], pqp_data[idx], quota_data[idx], bids_received_data[idx]])
            
        
        final_df = pd.DataFrame(data = temp_data, columns = column_names)




        return final_df
    

        # ##First we need to get the first and third wednesday within the window 
        # first_third_wed_date = DateTimeManager.get_nth_defined_day(self.window, self.interval, 'wednesday', 1)
        # thrid_wed_date = DateTimeManager.get_nth_defined_day(self.window, self.interval, 'wednesday', 3)

        # ##Next we need to check if it is a public holiday or not 
        # fuse = 7
        # continue_1 = False 
        # if self.holiday_checker.get(first_wed_date) != None:
        #     continue_1 = True 
    
        # while continue_1 == True:
        #     first_wed_date = DateTimeManager.object_to_string(DateTimeManager.string_to_object(first_wed_date) + DateTimeManager.time_delta(self.interval))

        #     continue_1 = False
        #     if self.holiday_checker.get(first_wed_date) != None:
        #         continue_1 = True 
            
        #     if DateTimeManager.string_to_object(first_wed_date).weekday() in [5, 6]:
        #         continue_1 = True

        #     fuse = fuse + 1
        #     if fuse <= 0:
        #         raise Exception ('Fuse-limit reached! Please review implementation... ...')