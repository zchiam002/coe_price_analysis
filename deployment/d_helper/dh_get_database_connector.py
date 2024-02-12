##Class for getting the database connection (matrix server)
import os 
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..', '')) + '//'
import sys 
sys.path.append(root_dir)
configuration_file_dir = root_dir + 'deployment//d_configurations//'
import json 

from data_manager.data_manager import DataManager

class DHDatabaseConnector:
    ##The constructor 
    def __init__ (self, connection_details_json):
        self.connection_details_json = connection_details_json

        with open (configuration_file_dir + connection_details_json['filename'], 'r') as read_file:
            self.table_route_optimizer_json = json.load(read_file)

        ##Initialize the connectors 
        self.connection_details = {}

        for idx in range (0, len(self.table_route_optimizer_json['table_descriptions'])):
            curr_table = self.table_route_optimizer_json['table_descriptions'][idx]
            curr_table_name = curr_table['name']
            curr_table_details = curr_table['column_details']

            curr_table_connection, curr_table_cursor = DataManager.Postgresql.get_table_connection_and_cursor(self.connection_details_json, curr_table_name, curr_table_details)

            self.connection_details[curr_table_name] = {'connection': curr_table_connection,
                                                        'cursor': curr_table_cursor}
    
    ######################################################################
    ##Method to close all connections 
    def close_all_connections (self):
        
        table_name_list = list(self.connection_details.keys())

        for table_name in table_name_list:
            self.connection_details[table_name]['cursor'].close()
            self.connection_details[table_name]['connection'].close()
        
        return 

    ######################################################################
    ##Method to get the connection and the cursor by table name
    def get_table_details (self, table_name):
        ret_details = None

        for table_detail in self.table_route_optimizer_json['table_descriptions']:
            if table_detail['name'] == table_name:
                ret_details = table_detail
                break 
        
        if ret_details == None:
            raise Exception ('Table name of ' + str(table_name) + 'is not found in the records... ... Please review implementation... ...')

        return ret_details 

