import yaml
from linksafety.exception import LinksafetyException
import sys
from linksafety.constant import *
def read_yaml_file(file_path:str)->dict:
    """
    Reads a YAML file and returns the contents as a dictionary.
    file_path: str
    """
    try:
        with open(file_path, 'rb') as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as ex:
        raise LinksafetyException(ex,sys) from ex