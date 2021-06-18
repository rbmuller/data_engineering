import os
import json
import boto3
import glob
from datetime import datetime 
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from hooks.prod_meetime_hook import MeetimeHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

class MeetimeRecentsOperator(BaseOperator):
    """
    Este Operator cria um arquivo json com os dados  da API do Meetime
    """

    def __init__(
        self,
        item,
        since_timestamp,
        connection_id,
        s3_connection_id,
        *args,
        **kwargs
    ):

        super().__init__(*args, **kwargs)

        self.item = item
        self.since_timestamp = since_timestamp[:19] #pegando apenas o timestamp
        self.connection_id = connection_id
        self.s3_connection_id = s3_connection_id
        self.hook = MeetimeHook(connection_id)

    def get_data(self):
        """
        Get data from the meetime API. Returns a single list containing data from all available pages.
        """
        data = []
        response = self.hook.send_request(item=self.item, since_timestamp=self.since_timestamp)
        if response['status_code'] == 200:
            self.log.info(f'API request successful. Using `since_timestamp={self.since_timestamp}`')
            if response['data']['data']:
                self.log.info('Found updates, appending data')
                data.extend(response['data']['data'])
            
                pagination = response['data']['next']
                counter = 2
        
                while pagination:
                    self.log.info('Additional page available')
                    response = self.hook.send_request(item=self.item, pagination_string=pagination)
                    if response['status_code'] == 200:
                        self.log.info(f'API request for page {counter} successful')
                        data.extend(response["data"]['data'])
                        pagination = response['data']['next']
                        counter += 1   
                    else:
                        self.log.error('API request failed')
                    
                self.log.info('Ingestion process successful!')

                endpoints = ('calls', 'demos', 'prospections/activities')
                exceptions = ('leads/custom-fields','prospections/lost-reasons')
            
                #Necessary steps needed due to different field names according to each endpoint

                if [ele for ele in endpoints if(ele in response['pag'])]:
                    last_updated_line = [data_dict['updated'] for data_dict in response['data']['data'] if data_dict['updated'] is not None]
                    last_update_timestamp = (max(sorted(last_updated_line)))
                    last_update_timestamp = datetime.strptime(last_update_timestamp, '%Y-%m-%dT%H:%M:%S.%fZ').strftime("%Y-%m-%d %H:%M:%S")

                elif 'leads' in response['pag'] not in exceptions:
                    last_updated_lead = [data_dict['lead_created_date'] for data_dict in response['data']['data'] if data_dict['lead_created_date'] is not None]
                    last_update_timestamp = (max(sorted(last_updated_lead)))  
                    last_update_timestamp = datetime.strptime(last_update_timestamp, '%Y-%m-%dT%H:%M:%S.%fZ').strftime("%Y-%m-%d %H:%M:%S")

                elif 'prospections' in response['pag'] not in exceptions:
                    last_updated_activity = [data_dict['created_date'] for data_dict in response['data']['data'] if data_dict['created_date'] is not None]
                    last_update_timestamp = (max(sorted(last_updated_activity)))
                    last_update_timestamp = datetime.strptime(last_update_timestamp, '%Y-%m-%dT%H:%M:%S.%fZ').strftime("%Y-%m-%d %H:%M:%S")
                
                else:
                    last_update_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")     
            
            #The last_update line comes in the UTC timestamp format, converted to datetime and then to easy reading timestamp format
            #standardizing according the else clause above, UTC NOW (datetime) to the same timestamp format
            
                response_dict = {
                    'data': data, 
                    'last_update_timestamp': last_update_timestamp
                }

            else:
                self.log.info('No updates happened, nothing to save')
            
                response_dict = {
                    'data': data, 
                    'last_update_timestamp': self.since_timestamp
                }       
        
            return response_dict
        else:
            self.log.error('API request failed')

    def upload_data(self, data, last_update_timestamp, request_timestamp):
        
        year = request_timestamp.strftime('%Y')
        month = request_timestamp.strftime('%m')
        day = request_timestamp.strftime('%d')
        hour = request_timestamp.strftime('%H')
        request_timestamp_string = request_timestamp.strftime('%Y%m%d%H%M%S')
        last_update_timestamp_string = datetime.strptime(last_update_timestamp, "%Y-%m-%d %H:%M:%S").strftime('%Y%m%d%H%M%S')
        filename = f"{last_update_timestamp_string}-{request_timestamp_string}.json"
        
        if self.item == 'leads/custom-fields':
            self.item = 'leads_custom-fields'
        
        elif self.item == 'prospections/activities':
            self.item = 'prospections_activities'
        
        elif self.item == 'prospections/lost-reasons':
            self.item = 'prospections_lost-reasons'
        
        key = f"meetime/{self.item}/year={year}/month={month}/day={day}/hour={hour}/{filename}"
        bucket = 's3-movidesk-datalake-dev-raw'
        s3 = S3Hook(self.s3_connection_id)
        s3.load_string(string_data=json.dumps(data), key=key, bucket_name=bucket)
        self.log.info(f'File {key} saved at {bucket}')
    
    def execute(self, context):
        """
        Retira dados da API e salva em um arquivo json na pasta Temp da VM
        """
        request_timestamp = datetime.utcnow()
        response_dict = self.get_data()

        if response_dict["data"]:
            self.upload_data(
                data=response_dict["data"], 
                last_update_timestamp=self.since_timestamp, 
                request_timestamp=request_timestamp)
            
            variable_key = f"meetime_last_update_timestamp_{self.item}"
            variable_value = response_dict["last_update_timestamp"]
            Variable.set(variable_key, variable_value)
            self.log.info(f"Changed variable '{variable_key}' to '{variable_value}'")
