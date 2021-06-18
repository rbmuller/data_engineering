from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
import requests
from datetime import timedelta, datetime 


class MeetimeHook(BaseHook):
    """
    Hook que interage com a API do Meetime
    """

    def __init__(self, connection_id, **kwargs):

        connection = BaseHook.get_connection(connection_id)

        if not connection.host:
            self.log.warning(
                "Conexão %s precisa ter a url no campo Host.",
                connection.id,
            )

        self.url = connection.host
        self.password = connection.password

    def send_request(self, item=None, since_timestamp=None, pagination_string=None):
        
        """
        Request for API Meetime 
        The date field for last update control has 4 different names accordinly its entity, the IF/ELSE clause deals with it.
        """ 
        headers = {
            'Accept': 'application/json',
            'Authorization': self.password
        }

        if pagination_string:
            page_url = self.url + pagination_string
            response = requests.get(page_url, headers=headers)
        else:
            updated_bf_af = ['calls', 'demos', 'prospections/activities']
            
            since_timestamp = datetime.strptime(since_timestamp, '%Y-%m-%d %H:%M:%S')
            since_timestamp = since_timestamp + timedelta(seconds=1, minutes=0, hours=3)
            since_timestamp = since_timestamp.strftime("%Y-%m-%d %H:%M:%S")

            parameters = {}

            if item in updated_bf_af:
                parameters['updated_after'] = since_timestamp
            elif item == 'leads':
                parameters['lead_created_after'] = since_timestamp
            elif item == 'prospections':
                parameters['last_activity_after'] = since_timestamp  
            
            response = requests.get(self.url + item, headers=headers, params=parameters)

        if item is not None:
            pag = item
        else:
            pag = pagination_string      

        response_dict = {
            'data': response.json(),
            'status_code': response.status_code,
            'pag': pag
        }
        
        return response_dict
    