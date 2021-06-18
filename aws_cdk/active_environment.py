import os 
from enum import Enum 

class Environment(Enum):
    PRODUCTION = 'production'
    DEV = 'dev'
    STAGE = 'stage'

active_environment = Environment[os.environ['ENVIRONMENT']]


