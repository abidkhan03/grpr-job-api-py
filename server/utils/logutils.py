from utils.socketutils import *
import logging
from utils.constants.nodeconstants import *


class SocketHandler(logging.StreamHandler):

    def __init__(self, server_url, event):
        logging.StreamHandler.__init__(self)
        self.server_url = server_url
        self.event = event

    def emit(self, record):
        content = {
            name: value
            for name, value in record.__dict__.items()
        }
        content = content["msg"]
        connect_to_socket(self.server_url)
        send_signal(event=self.event, data=content)


def get_signal_logger(name: str):
    """
    This logger is used to log signals sent by the server
    :param name: name of the logger
    :return: logger
    """
    logger = logging.getLogger(name)
    ch = logging.StreamHandler()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(levelname)s] - %(message)s')
    ch.setFormatter(formatter)
    sh = SocketHandler(node_server_url, log_channel)
    logger.addHandler(ch)
    logger.addHandler(sh)
    return logger


def get_progress_logger(name: str):
    """
    This logger is used to log progress of the job
    :param name: name of the logger
    :return: logger
    """
    logger = logging.getLogger(name)
    ch = logging.StreamHandler()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(levelname)s] - %(message)s')
    ch.setFormatter(formatter)
    sh = SocketHandler(node_server_url, progress_channel)
    logger.addHandler(ch)
    logger.addHandler(sh)
    return logger


def get_job_logger(name: str):
    """
    This logger is used to log logs of the job
    :param name: name of the logger
    :return: logger
    """
    logger = logging.getLogger(name)
    ch = logging.StreamHandler()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(levelname)s] - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


signal_logger = get_signal_logger('job_signal')
job_logger = get_job_logger('job')
progress_logger = get_progress_logger('job_progress')
