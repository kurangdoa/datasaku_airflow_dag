import boto3
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.hooks.base import BaseHook
from airflow.models import Variable

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


# conn = BaseHook.get_connection('aws_default')
# LoggingMixin().log.info(conn.get_extra())
boto = ConnS3(aws_access_key_id = Variable.get("AWS_SECRET"), aws_secret_access_key = Variable.get("AWS_KEY"))
my_bucket = boto.s3_list_bucket()
LoggingMixin().log.info(my_bucket)