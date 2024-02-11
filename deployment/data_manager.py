##The main class for managing all data related stuff 
import os 
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '')) + '//'
template_dir = root_dir + 'data_manager//default_configurations//'
import json
import psycopg2
import pandas as pd 

from ancillaries.date_time_manager import DateTimeManager

class DataManager:
    ######################################################################
    ##Sub-class for managing the postgresql related matters 
    class Postgresql:
        ##################################################################
        ##INTERNAL: Method to return the string version of the value given a datatype 
        def __process_into_string (input_value, data_type):
            ret_value = None
            if data_type in ['NUMERIC']:
                if input_value != None:
                    ret_value = str(input_value)
                else: 
                    ret_value = str(-1.0)
            elif data_type in ['CHAR', 'TEXT', 'BOOLEAN']:
                ret_value = '\'' + str(input_value) + '\''
            elif data_type == 'TIMESTAMPTZ':
                ret_value = '\'' + DateTimeManager.object_to_string(input_value, date_separator = '-').replace(', ', 'T') + 'Z' + '\''
            else:
                raise Exception ('Unknown data type of ' + str(data_type) + ' encountered... ... Please review implementation... ...')

            return ret_value
        ##################################################################
        ##Method for querying for data 
        def query (connection, cursor, query_string, column_details):

            ##Executing the query
            cursor.execute(query_string)
            connection.commit()

            output = cursor.fetchall()

            return pd.DataFrame(data = output, columns = column_details['names'])

        ##################################################################
        ##Method for ingesting data into a table with a specified format 
        def ingest_data (connection, cursor, table_name, column_details, data_list):

            ##Building the query
            query = 'INSERT INTO ' + table_name + ' ('
            for idx in range(0, len(column_details['names'])):
                query = query + column_details['names'][idx]

                if idx < len(column_details['names']) - 1:
                    query = query + ', '

            query = query + ') VALUES \n'

                ##Formatting the data 
            for idx in range (0, len(data_list)):
                curr_str = '('
                for idx_1 in range (0, len(data_list[idx])):
                    curr_data_type = column_details['data_types'][idx_1]
                    curr_value = DataManager.Postgresql.__process_into_string(data_list[idx][idx_1], curr_data_type)

                    curr_str = curr_str + curr_value

                    if idx_1 < len(data_list[idx]) - 1:
                        curr_str = curr_str + ', '

                curr_str = curr_str + ')'

                if idx < len(data_list) - 1:
                    curr_str = curr_str + ', \n'
                else:
                    curr_str = curr_str + '\n'
                
                query = query + curr_str

            cursor.execute(query)
            connection.commit()

            return
        
        ##################################################################
        ##Method for getting the table connection and cursor
        def get_table_connection_and_cursor (connection_details_json, table_name, column_details):

            connection = psycopg2.connect(host = connection_details_json['host'], 
                                          dbname = connection_details_json['dbname'],
                                          user = connection_details_json['user'], 
                                          password = connection_details_json['password'], 
                                          port = connection_details_json['port'])
            
            cursor = connection.cursor()

            ##Creating the table if it does not exist 
            column_names = column_details['names']
            column_data_types = column_details['data_types']
                ##The first column has to be the primary key 
            assert column_details['is_primary_key'][0] == True
            assert len(column_names) == len(column_data_types)

            query = 'CREATE TABLE IF NOT EXISTS ' + str(table_name) + ' ( \n'
            for idx in range (0, len(column_names)):
                query = query + str(column_names[idx]) + ' ' + str(column_data_types[idx])

                if idx == 0:
                    query = query + ' PRIMARY KEY'
                if idx < len(column_names) - 1:
                    query = query + ', \n'
            
            query = query + '); \n'

            cursor.execute(query)
            connection.commit()

            return connection, cursor 
        
        ##################################################################
        ##Method to get the connection details template 
        def get_connection_details_template_json ():
            connection_template_dir = template_dir + 'postgres_connection_details_template.json'

            with open (connection_template_dir, 'r') as read_file:
                connection_template_json = json.load(read_file)

            return connection_template_json