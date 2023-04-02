import boto3
from airflow.utils.log.logging_mixin import LoggingMixin

class ConnS3:
    """Class for S3"""
    def __init__(self, aws_access_key_id, aws_secret_access_key, aws_session_token = None):
        self.aws_id =  aws_access_key_id
        self.aws_key = aws_secret_access_key
        self.aws_token = aws_session_token

        # create session
        if (self.aws_id is not None) & (self.aws_key is not None):
            self.s3_session = boto3.Session(
                aws_access_key_id = self.aws_id,
                aws_secret_access_key = self.aws_key,
                aws_session_token = self.aws_token,
            ) # create aws session if there is aws credential defined
        else:
            self.s3_session = boto3.Session() # create aws session using using environment detail 

    def s3_list_bucket(self):
        """list bucket inside s3"""
        s3_resource = self.s3_session.resource('s3') # create s3 resource
        buckets = [bucket.name for bucket in s3_resource.buckets.all()] # list down the bucket
        return buckets

LoggingMixin().log.info(ConnS3(aws_access_key_id = 'AKIARRZC2LLJLYNONUUA', aws_secret_access_key = '1Vf0jXnItOJqj6ygKWUL%2Bb6FloXgjm6nbIDSL94H').s3_list_bucket())
