from flask import Flask
from linksafety.logger import logging
from linksafety.exception import LinksafetyException
import sys
app = Flask(__name__)

@app.route("/",methods=['GET','POST'])

def index():
    try:
        raise Exception("Custom EXCEPTION TESTING")
    except Exception as ex:
        linksafety = LinksafetyException(ex,sys)
        logging.info(linksafety.error_message)
        logging.info("Testing Logging")
    return "Machine Learning Project"

if __name__=="__main__":
    app.run(debug=True)