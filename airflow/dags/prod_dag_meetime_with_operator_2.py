import os
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.dummy import DummyOperator
from airflow.exceptions import AirflowSkipException
from operators.prod_meetime_operator import MeetimeRecentsOperator
from datetime import datetime

default_args = {
    'owner': 'Robson Müller',
    'catchup': False
}

@dag(
    default_args=default_args, 
    schedule_interval="0 5 * * *", 
    start_date=datetime(2021, 1, 1, 12, 0),
    max_active_runs=1, 
    tags=['meetime']
)
def meetime_slowly_changing_dimensions():
    """
    Exctraction of Meetime dimensions which has no control for updates,
    these dimensions are being uploaded once a day at 17:00 (check the cron expression above) 
    
    """

    items = ['users' 
            ,'cadences' 
            ,'company']

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end',trigger_rule='none_failed')

    with TaskGroup(group_id="meetime") as tg:
        for item in items:
            extract_recents = MeetimeRecentsOperator(
                task_id= str('extract_' + item),
                item=item,
                since_timestamp=Variable.get(f"meetime_last_update_timestamp_{item}", default_var='2021-01-01 00:00:00'),
                s3_connection_id='movilake',
                connection_id='meetime_api'
            )

            extract_recents
            
    start >> tg >> end

tutorial_etl_dag = meetime_slowly_changing_dimensions()