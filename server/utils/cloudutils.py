from google.cloud import storage
import gcsfs
from utils.constants.cloudconstants import *
import pandas as pd


def upload_to_google_cloud(file_path: str, upload_file_path: str):
    """
    Uploads a file to Google Cloud Storage
    :param file_path: The path to the file to be uploaded
    :param upload_file_path: The path to the file to be uploaded to
    """
    client = storage.Client()
    bucket = client.get_bucket(gcloud_bucket_name)
    blob = bucket.blob(upload_file_path)
    blob.upload_from_filename(file_path)


def read_csv_from_google_cloud(file_path: str):
    """
    Reads a CSV file from Google Cloud Storage
    :param file_path: The path to the file to be read
    :return: The CSV file as a Pandas DataFrame
    """
    fs = gcsfs.GCSFileSystem(project=gcloud_project_name)
    with fs.open(f'{gcloud_bucket_name}/{file_path}') as f:
        df = pd.read_csv(filepath_or_buffer=f, encoding='utf-8-sig')
    return df
