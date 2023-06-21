from utils.logutils import *
from datetime import datetime

import socketio

sio = socketio.Client()


def connect_to_socket(url):
    """
    Connect to the socket server
    :param url: url of the socket server
    :return: None
    """
    if not sio.connected:
        try:
            sio.connect(url=url, transports=["websocket"])
        except Exception as e:
            job_logger.error(f"Could not establish connection to socket: {e}")
            return


def send_signal(event, data):
    """
    Send a signal to the socket server
    :param event: event to send
    :param data: data to send
    :return: None
    """
    try:
        sio.emit(event=event, data=data)
    except Exception as e:
        job_logger.error(f"Could not establish connection to socket: {e}")
        return


def get_time_stamp():
    """
    Get the current time stamp
    :return: current time stamp
    """
    now = datetime.now()
    return now.strftime('%Y-%m-%d %H:%M:%S')
