import logging 
import datetime

logging.basicConfig(filename='errors_log.log', level=logging.ERROR)


def log_error(message):
    if not type(message) is str:
        message = str(message)
    logging.error(str(datetime.datetime.now()) + ':' + message)