import sys
sys.path.insert(1, '/Users/rhyando.anggoro-adi/Library/CloudStorage/OneDrive-Personal/code/helm/airflow_custom/datasaku_airflow_dag/dags/samsung')
import utils.kaggle as kaggle
import pandas as pd
from airflow.models import Variable

kaggle = kaggle.kaggle_api(Variable.get("KAGGLE_USERNAME"), Variable.get("KAGGLE_KEY"))
kaggle.download_dataset('lipann/prepaired-data-of-customer-revenue-prediction')

# moda means the categorical feature stated as integer 
bronze_train_categorical_features_moda = pd.read_csv('train_categorial_features_moda.csv')
print(bronze_train_categorical_features_moda.head())