from datasaku import datasaku_minio
import os
from airflow.utils.log.logging_mixin import LoggingMixin
import pandas as pd
from airflow.models import Variable

minio_access = os.getenv("AIRFLOW_VAR_MINIO_ACCESS")
minio_secret = os.getenv("AIRFLOW_VAR_MINIO_SECRET")
LoggingMixin().log.info(minio_access)
LoggingMixin().log.info(minio_secret)
minio_access = Variable.get('MINIO_ACCESS')
minio_secret = Variable.get('MINIO_SECRET')
LoggingMixin().log.info(minio_access)
LoggingMixin().log.info(minio_secret)

minio = datasaku_minio.ConnMinio(
    minio_host = "host.docker.internal:9000",
    minio_access_key=minio_access,
    minio_secret_key=minio_secret
)

my_bucket = minio.minio_list_bucket()
LoggingMixin().log.info(my_bucket)

dict = {"a":1, "b":2, "c":3}
df = pd.DataFrame(dict, index=[0])

minio.minio_upload_file(minio_bucket_def = "test-bucket", minio_prefix_def = "test/", df_def = df, file_def = "test.csv")