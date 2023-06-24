import sys
sys.path.append('.')
import utils.kaggle as kaggle
import pandas as pd

kaggle = kaggle.kaggle_api('rhyando', '78b024bccc15da848c538f19199d9f03')
kaggle.download_dataset('lipann/prepaired-data-of-customer-revenue-prediction')

# moda means the categorical feature stated as integer 
bronze_train_categorical_features_moda = pd.read_csv('train_categorial_features_moda.csv')
print(bronze_train_categorical_features_moda.head())