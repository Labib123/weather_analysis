import boto3
from botocore.exceptions import ClientError


class DataStoreClient:
    """
    A class for reading and writing data to an S3 bucket.
    """

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3 = boto3.resource('s3')

    def read_data(self, file_path):
        try:
            obj = self.s3.Object(self.bucket_name, file_path)
            return obj.get()['Body'].read()
        except ClientError as e:
            print(f"Error reading data from {file_path}: {e}")
            return None

    def write_data(self, data, file_path):
        try:
            obj = self.s3.Object(self.bucket_name, file_path)
            obj.put(Body=data)
            print(f"Data written to {file_path} in {self.bucket_name}")
        except ClientError as e:
            print(f"Error writing data to {file_path}: {e}")
