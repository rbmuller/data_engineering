
#Dictionary of additional policies to s3 Buckets

##CREATION OF POLICIES DURING BASE DEFINITION
'''
self.set_default_lifecycle_rules()

def set_default_lifecycle_rules(self):
        """
        Sets lifecycle rule by default
        """
        self.add_lifecycle_rule(
            abort_incomplete_multipart_upload_after=core.Duration.days(7), 
            enabled=True
        )
        
        self.add_lifecycle_rule(
            noncurrent_version_transitions=[
                s3.NoncurrentVersionTransition(
                    storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                    transition_after=core.Duration.days(45),
                ),
                s3.NoncurrentVersionTransition(
                    storage_class=s3.StorageClass.GLACIER,
                    transition_after=core.Duration.days(90),
                ),
            ]
        )

        self.add_lifecycle_rule(noncurrent_version_expiration=core.Duration.days(360))    

##CREATION OF POLICIES DURING STACK CREATION

self.data_lake_raw_bucket.add_lifecycle_rule(
    transitions=[
        s3.Transitions(
            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
            transition_after=core.Duration.days(90)
        ),
        s3.Transition(
            storage_class=s3.StorageClass.GLACIER,
            transition_after=core.Duration.days(360)
        )
    ],
    enabled=True
)
'''
