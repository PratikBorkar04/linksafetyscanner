from linksafety.entity.config_entity import DataIngestionConfig
from linksafety.exception import LinksafetyException
import sys,os
from linksafety.logger import logging

class DataIngestion:
    def __init__(self,data_ingestion_config:DataIngestionConfig):
        try:
            logging.info(f"{'>>'*20}Data Ingestion log started.{'<<'*20} ")
            self.data_ingestion_config = data_ingestion_config
        except Exception as ex:
            raise LinksafetyException(ex,sys)
        
    def initiate_data_ingestion(self)-> DataIngestionArtifact:
        try:
            tgz_file_path =  self.download_housing_data()
            self.extract_tgz_file(tgz_file_path=tgz_file_path)
            return self.split_data_as_train_test()
        except Exception as ex:
            raise LinksafetyException(ex,sys) from ex