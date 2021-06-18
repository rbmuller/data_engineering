from aws_cdk import core 
from aws_cdk import aws_s3 as s3 
from infra_cdk.datalake.base import BaseDataLakeBucket, DataLakeLayer
from infra_cdk.active_environment import active_environment

#Creation of both buckets
class DataLakeStack(core.Stack):
    def __init__(self, scope: core.Construct, **kwargs) -> None:
        self.deploy_env = active_environment
        super().__init__(scope, id=f'{self.deploy_env.value}-data-lake-stack' ,**kwargs)
        
        self.data_lake_raw_bucket = BaseDataLakeBucket(
            self,
            layer=DataLakeLayer.RAW

        )
        
       
        self.data_lake_struct_bucket = BaseDataLakeBucket(
            self,
            layer=DataLakeLayer.STRUCTURED

        )
    
    
        
        