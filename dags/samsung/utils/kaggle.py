from kaggle.api.kaggle_api_extended import KaggleApi
import os

class kaggle_api:
    def __init__(self, username, key):
        os.environ['KAGGLE_USERNAME'] = username
        os.environ['KAGGLE_KEY'] = key
        self.api = KaggleApi()
        self.api.authenticate()

    def download_dataset(self, kaggle_path):
        self.api.dataset_download_files(kaggle_path, unzip=True)