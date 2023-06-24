import sys
sys.path.insert(1, '/opt/airflow/dags/repo/dags/samsung/')
#sys.path.insert(1, '/Users/rhyando.anggoro-adi/Library/CloudStorage/OneDrive-Personal/code/helm/airflow_custom/datasaku_airflow_dag/dags/samsung')
import utils.datasaku_sqlalchemy as datasaku_sqlalchemy
import pandas as pd
# from airflow.models import Variable
from kaggle.api.kaggle_api_extended import KaggleApi
import os
import zipfile

# connection to kaggle
print('connection to kaggle')
# os.environ['KAGGLE_USERNAME'] = Variable.get("KAGGLE_USERNAME")
# os.environ['KAGGLE_KEY'] = Variable.get("KAGGLE_KEY")
api = KaggleApi()
api.authenticate()
api.dataset_download_files('lipann/prepaired-data-of-customer-revenue-prediction')

# df = pd.read_csv('prepaired-data-of-customer-revenue-prediction.zip', compression='zip')
with zipfile.ZipFile("prepaired-data-of-customer-revenue-prediction.zip") as zipf:
   print(zipf.namelist())
   train_dataset = [s for s in zipf.namelist() if "train_" in s]
   for file in train_dataset:
      with zipf.open(file) as f:
        content = f.read()
        f = open(file, 'wb')
        f.write(content)

# train_flat
train_flat = pd.read_csv('train_flat.csv')
print(train_flat.head())

##### bronze layer #####

# Bronze layer is the layer as close as possible to the raw data for troubleshooting purpose
# Schema adjustment is done in Bronze layer as well to match organization fit.

fct_bronze_google_analytics = train_flat

# based on data provided, there are two main dataset, flat and filtered
# filtered dataset <> flat dataset (with filter) -- because there are records in filtered dataset that not exist in flat dataset
# for example fullvisitorid 7530866178634633311
# decided to use flat dataset as main dataset because of long term reason of column availability compared to filtered dataset such as 
#  'sessionId',
#  'socialEngagementType',
#  'visitId',
#  'totals_visits',
#  'device_browserSize',
#  'device_browserVersion',
#  'device_flashVersion',
#  'device_language',
#  'device_mobileDeviceBranding',
#  'device_mobileDeviceInfo',
#  'device_mobileDeviceMarketingName',
#  'device_mobileDeviceModel',
#  'device_mobileInputSelector',
#  'device_operatingSystemVersion',
#  'device_screenColors',
#  'device_screenResolution',
#  'geoNetwork_cityId',
#  'geoNetwork_latitude',
#  'geoNetwork_longitude',
#  'geoNetwork_networkLocation',
#  'trafficSource_adwordsClickInfo.criteriaParameters',
#  'trafficSource_adwordsClickInfo.gclId',
#  'trafficSource_campaignCode'

##### save to sql #####
print('saving to sql')
samsung = datasaku_sqlalchemy.sqlalchemy_class(host = 'host.docker.internal', username = 'postgres', port = 5555)
samsung.execute_create_database('samsung')
samsung = datasaku_sqlalchemy.sqlalchemy_class(host = 'host.docker.internal', username = 'postgres', port = 5555, database = 'samsung')
# samsung = datasaku_sqlalchemy.sqlalchemy_class(host = 'localhost', username = 'postgres', port = 5555, database = 'samsung')
samsung.execute_query ("""CREATE SCHEMA IF NOT EXISTS bronze""")
# samsung.pandas_to_sql(df = fct_bronze_google_analytics, table_name = 'fct_bronze_google_analytics', schema_name = 'bronze', if_exists = 'replace')
# test = samsung.sql_to_pandas("""SELECT * FROM bronze.fct_bronze_google_analytics LIMIT 5;""")
# print(test.head())

