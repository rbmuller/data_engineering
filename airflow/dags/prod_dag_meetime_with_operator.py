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
    schedule_interval="*/10 * * * *",
    start_date=datetime(2021, 1, 1, 12, 0),
    max_active_runs=1, 
    tags=['meetime']
)
def meetime_incremental():
    """
    Incremental extraction of bellow entities from Meetime API
   
    """

    items = ['calls'
            ,'demos'
            ,'leads'
            ,'leads/custom-fields'
            ,'prospections'
            ,'prospections/activities'
            ,'prospections/lost-reasons'
            ]

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end',trigger_rule='none_failed')

    with TaskGroup(group_id="meetime") as tg:
        for item in items:
            #Airflow does not support slashes as task id
            item_remove_slash = item.replace('/','_')
            extract_recents = MeetimeRecentsOperator(
                task_id= str('extract_' + item_remove_slash),
                item=item,
                since_timestamp=Variable.get(f"meetime_last_update_timestamp_{item}", default_var='2021-01-01 00:00:00'),
                s3_connection_id='movilake',
                connection_id='meetime_api'
            )

            extract_recents
            
    start >> tg >> end

tutorial_etl_dag = meetime_incremental()