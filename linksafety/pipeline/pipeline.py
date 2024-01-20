from linksafety.config.configuration import Configuartion
from linksafety.logger import logging
from linksafety.exception import LinksafetyException
from linksafety.entity.config_entity import DataIngestionConfig
from linksafety.entity.artifact_entity import DataIngestionArtifact
from linksafety.component.data_ingestion import DataIngestion

import os,sys

class Pipeline:
    def __init__(self,config:Configuartion=Configuartion()) -> None:
        try:
            self.config = config
        except Exception as ex:
            raise LinksafetyException(ex,sys) from ex
        
    def start_data_ingestion(self)->DataIngestionArtifact:
        try:
            data_ingestion = DataIngestion(data_ingestion_config=self.config.get_data_ingestion_config())
            return data_ingestion.initiate_data_ingestion()
        except Exception as ex:
            raise LinksafetyException(ex,sys) from ex
    
    def start_data_validation(self):
        try:
            pass
        except Exception as ex:
            raise LinksafetyException(ex,sys) from ex
    def start_data_transformation(self):
        try:
            pass
        except Exception as ex:
            raise LinksafetyException(ex,sys) from ex
    def start_model_trainer(self):
        try:
            pass
        except Exception as ex:
            raise LinksafetyException(ex,sys) from ex
    def start_model_evaluation(self):
        try:
            pass
        except Exception as ex:
            raise LinksafetyException(ex,sys) from ex
    def start_model_pusher(self):
        try:
            pass
        except Exception as ex:
            raise LinksafetyException(ex,sys) from ex
    def run_pipeline(self):
        try:
            data_ingestion_artifact = self.start_data_ingestion()
        except Exception as ex:
            raise LinksafetyException(ex,sys) from ex
