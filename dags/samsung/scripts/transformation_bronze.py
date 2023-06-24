import sys
sys.path.insert(1, '/opt/airflow/dags/repo/dags/samsung/')
import utils.kaggle as kaggle
import pandas as pd
from airflow.models import Variable
import os

os.environ['KAGGLE_USERNAME'] = Variable.get("KAGGLE_USERNAME")
os.environ['KAGGLE_KEY'] = Variable.get("KAGGLE_KEY")

from kaggle.api.kaggle_api_extended import KaggleApi

api = KaggleApi()
api.authenticate()
api.dataset_download_files('lipann/prepaired-data-of-customer-revenue-prediction', unzip=True)

# moda means the categorical feature stated as integer 
bronze_train_categorical_features_moda = pd.read_csv('train_categorial_features_moda.csv')
print(bronze_train_categorical_features_moda.head())