import sys
sys.path.insert(1, '/opt/airflow/dags/repo/dags/samsung/')
import utils.datasaku_sqlalchemy as datasaku_sqlalchemy
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile

# connection to kaggle
print('connection to kaggle')
api = KaggleApi()
api.authenticate()
api.dataset_download_files('lipann/prepaired-data-of-customer-revenue-prediction')

# unzip file
with zipfile.ZipFile("prepaired-data-of-customer-revenue-prediction.zip") as zipf:
   print(zipf.namelist())
   train_dataset = [s for s in zipf.namelist() if "train_" in s]
   for file in train_dataset:
      with zipf.open(file) as f:
        content = f.read()
        f = open(file, 'wb')
        f.write(content)

##### bronze layer #####

# Bronze layer is the layer as close as possible to the raw data for troubleshooting purpose
# Schema adjustment is done in Bronze layer as well to match organization fit.

# train_flat
train_flat = pd.read_csv('train_flat.csv')
fct_bronze_google_analytics_flat = train_flat

train_filtered = pd.read_csv('train_filtered.csv')
fct_bronze_google_analytics_filtered = train_filtered

train_category = pd.read_csv('train_categorial_features_moda.csv')
fct_bronze_google_analytics_category = train_category

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

##### save to sql database #####

print('saving to sql')
samsung = datasaku_sqlalchemy.sqlalchemy_class(host = 'host.docker.internal', username = 'postgres', port = 5555)
samsung.execute_create_database('samsung')
samsung = datasaku_sqlalchemy.sqlalchemy_class(host = 'host.docker.internal', username = 'postgres', port = 5555, database = 'samsung')
samsung.execute_query ("""CREATE SCHEMA IF NOT EXISTS bronze""")
samsung.pandas_to_sql(df = fct_bronze_google_analytics_flat, table_name = 'fct_bronze_google_analytics_flat', schema_name = 'bronze', if_exists_remark = 'replace')
samsung.pandas_to_sql(df = fct_bronze_google_analytics_filtered, table_name = 'fct_bronze_google_analytics_filtered', schema_name = 'bronze', if_exists_remark = 'replace')
samsung.pandas_to_sql(df = fct_bronze_google_analytics_category, table_name = 'fct_bronze_google_analytics_category', schema_name = 'bronze', if_exists_remark = 'replace')