from linksafety.entity.config_entity import DataIngestionConfig
from linksafety.entity.artifact_entity import DataIngestionArtifact
from linksafety.exception import LinksafetyException
import sys,os
from linksafety.logger import logging
from six.moves import urllib
import csv
import pandas as pd
from urllib.parse import urlparse, parse_qs
from sklearn.model_selection import StratifiedShuffleSplit



class DataIngestion:
    def __init__(self,data_ingestion_config:DataIngestionConfig):
        try:
            logging.info(f"{'>>'*20}Data Ingestion log started.{'<<'*20} ")
            self.data_ingestion_config = data_ingestion_config
            # Define new columns outside the function
            self.new_columns = ['hostname_length', 'path_length', 'fd_length', 'count_of_dash',
                        'count_of_at', 'count_of_question', 'count_of_percent', 'count_of_dot',
                        'count_of_equal', 'count_of_http', 'count_of_https', 'count_of_www',
                        'count_of_digits', 'count_of_letters', 'count_of_dir', 'use_of_ip',
                        'qty_hyphen_url', 'length_url', 'qty_tilde_url',
                        'qty_dot_url', 'qty_percent_url', 'length_domain', 'params_length',
                        'qty_and_params', 'qty_hyphens_params', 'directory_length',
                        'qty_equal_params', 'qty_equal_url', 'qty_slash_url',
                        'qty_slash_directory', 'file_length', 'qty_and_url', 'qty_dot_params']
        except Exception as ex:
            raise LinksafetyException(ex,sys)
    
    def download_linksafety_data(self,)->str:
        try:
            download_url = self.data_ingestion_config.dataset_download_url

            csv_download_dir = self.data_ingestion_config.csv_download_dir

            if os.path.exists(csv_download_dir):
                os.remove(csv_download_dir)

            os.makedirs(csv_download_dir,exist_ok=True)

            linksafety_file_name = os.path.basename(download_url)

            csv_file_path = os.path.join(csv_download_dir,linksafety_file_name)

            logging.info(f"Downloading file from :[{download_url}]into :[{csv_file_path}]")
            urllib.request.urlretrieve(download_url,csv_file_path)
            logging.info(f"File :[{csv_file_path}] has been downloaded Successfully")

            return csv_file_path
        except Exception as ex:
            raise LinksafetyException(ex,sys) from ex
    
    def extract_csv_file(self,csv_file_path:str):
        try:
            raw_data_dir = self.data_ingestion_config.raw_data_dir
            if os.path.exists(raw_data_dir):
                os.remove(raw_data_dir)
            os.makedirs(raw_data_dir,exist_ok=True)

            logging.info(f"Extracting csv file:[{csv_file_path}] into dir:[{raw_data_dir}]")
            with open(csv_file_path, 'r') as linksafety_csv_file_obj:
                    csv.reader(linksafety_csv_file_obj)
            logging.info(f"Extracting Completed")

        except Exception as ex:
            raise LinksafetyException (ex,sys) from ex
        
    def extract_url_features(self, url):
        # Your existing code for extract_url_features function
        parsed_url = urlparse(url)

        url_length = len(url)
        domain = parsed_url.netloc
        hostname_length = len(domain)

        path = parsed_url.path
        path_length = len(path)

        fd_length = len(url.split('/')[-1])
        count_of_dash = url.count('-')
        count_of_at = url.count('@')
        count_of_question = url.count('?')
        count_of_percent = url.count('%')
        count_of_dot = url.count('.')
        count_of_equal = url.count('=')
        count_of_http = url.count('http')
        count_of_https = url.count('https')
        count_of_www = url.count('www')
        count_of_digits = sum(c.isdigit() for c in url)
        count_of_letters = sum(c.isalpha() for c in url)
        count_of_dir = url.count('/')

        use_of_ip = 1 if domain.replace('.', '').isdigit() else 0

        # Additional columns
        qty_hyphen_url = url.count('-')
        length_url = len(url)
        qty_tilde_url = url.count('~')
        qty_dot_url = url.count('.')
        qty_percent_url = url.count('%')
        length_domain = len(domain)
        params_length = len(parse_qs(parsed_url.query))
        qty_and_params = url.count('&')
        qty_hyphens_params = url.count('-')
        directory_length = len(parsed_url.path.split('/'))
        qty_equal_params = url.count('=')
        qty_equal_url = url.count('=')
        qty_slash_url = url.count('/')
        qty_slash_directory = url.count('/') - 1
        file_length = len(parsed_url.path.split('/')[-1])
        qty_and_url = url.count('&')
        qty_dot_params = url.count('.')

        return [hostname_length, path_length, fd_length, count_of_dash,
                count_of_at, count_of_question, count_of_percent, count_of_dot,
                count_of_equal, count_of_http, count_of_https, count_of_www,
                count_of_digits, count_of_letters, count_of_dir, use_of_ip,
                qty_hyphen_url, length_url, qty_tilde_url, qty_dot_url,
                qty_percent_url, length_domain, params_length, qty_and_params,
                qty_hyphens_params, directory_length, qty_equal_params,
                qty_equal_url, qty_slash_url, qty_slash_directory, file_length,
                qty_and_url, qty_dot_params]

    def split_data_as_train_test(self):
        try:
            raw_data_dir = self.data_ingestion_config.raw_data_dir
            file_name = os.listdir(raw_data_dir)[0]
            linksafety_file_path = os.path.join(raw_data_dir, file_name)
            linksafety_data_frame = pd.read_csv(linksafety_file_path)

            # Apply the function to create new columns
            linksafety_data_frame[self.new_columns] = linksafety_data_frame['url'].apply(self.extract_url_features).apply(pd.Series)

            # Display the updated DataFrame
            print(linksafety_data_frame.head())

            logging.info(f"Splitting data into train and test")
            
            # Assuming "result" is your target variable
            strat_split = StratifiedShuffleSplit(n_splits=1, test_size=0.2, random_state=42)

            for train_index, test_index in strat_split.split(linksafety_data_frame, linksafety_data_frame["result"]):
                strat_train_set = linksafety_data_frame.loc[train_index].drop(self.new_columns, axis=1)
                strat_test_set = linksafety_data_frame.loc[test_index].drop(self.new_columns, axis=1)

            train_file_path = os.path.join(self.data_ingestion_config.ingested_train_dir,
                                            file_name)

            test_file_path = os.path.join(self.data_ingestion_config.ingested_test_dir,
                                        file_name)

            if strat_train_set is not None:
                os.makedirs(self.data_ingestion_config.ingested_train_dir,exist_ok=True)
                strat_train_set.to_csv(train_file_path,index=False)

            if strat_test_set is not None:
                os.makedirs(self.data_ingestion_config.ingested_test_dir,exist_ok=True)
                strat_test_set.to_csv(test_file_path,index=False)

            data_ingestion_artifact = DataIngestionArtifact(train_file_path=train_file_path,
                                    test_file_path=test_file_path,
                                    is_ingested=True,
                                    message=f"Data Ingestion completed Successfully"
                                )
            
            return data_ingestion_artifact
        except Exception as ex:
            raise LinksafetyException(ex, sys) from ex
        
    def initiate_data_ingestion(self)-> DataIngestionArtifact:
        try:
            csv_file_path =  self.download_linksafety_data()
            self.extract_csv_file(csv_file_path=csv_file_path)
            return self.split_data_as_train_test()
        except Exception as ex:
            raise LinksafetyException(ex,sys) from ex
        
    def __del__(self):
        logging.info(f"{'='*20}Data Ingestion og completed.{'='*20}\n\n")