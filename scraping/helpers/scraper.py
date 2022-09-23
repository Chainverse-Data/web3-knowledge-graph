import requests
import boto3
from botocore.exceptions import ClientError
import datetime
import logging
import json
import os

class Scraper:
    def __init__(self, bucket_name, allow_override=False):
        self.runtime = datetime.datetime.now()
        
        self.s3 = boto3.client('s3')
        self.bucket_name = os.environ["AWS_BUCKET_PREFIX"] + bucket_name
        self.bucket = self.create_or_get_bucket()
        
        self.data = {}
        self.data_filename = "data_{}-{}-{}.json".format(self.runtime.year, self.runtime.month, self.runtime.day)
        if not allow_override and self.check_if_file_exists(self.data_filename):
            logging.error("The data file for this day has already been created!")
            raise Exception("The data file for this day has already been created!")
        
        self.metadata_filename = "metadata.json"
        self.metadata = self.read_metadata()

    def run(self):
        "Main function to be called. Every scrapper must implement its own run function !"
        raise NotImplementedError("ERROR: the run function has not been implemented!")

    # This section handles requests and networking.

    def get_request(self, url, params=None, headers=None, counter=0):
        """This makes a GET request to a url and return the data. 
        It can take params and headers as parameters following python's request library.
        The method returns the raw request content, you must then parse the content with the correct parser."""
        if counter > 10:
            return None
        r = requests.get(url, params=params, headers=headers)
        if r.status_code != 200:
            self.get_request(url, params=params, headers=headers, counter=counter+1)
        return r.content

    def post_request(self, url, data=None, json=None, headers=None, counter=0):
        if counter > 10:
            return None
        r = requests.post(url, data=data, json=json, headers=headers)
        if r.status_code != 200:
            self.post_request(url, data=data, json=json, headers=headers, counter=counter+1)
        return r.content

    # This section contains functions to deal with S3 storage.

    def read_metadata(self):
        "Access the S3 bucket to read the metadata and returns a dictionary that corresponds to the saved JSON object"
        if self.check_if_file_exists(self.metadata_filename):
            return self.load_from_s3(self.metadata_filename)
        else:
            return {}
        
    def save_metadata(self):
        "Saves the current metadata to S3"
        self.save_to_s3(self.metadata_filename, self.metadata)

    def save_data(self):
        "Saves the current data to S3"
        self.save_to_s3(self.data_filename, self.data)
    
    def save_to_s3(self, filename, data):
        "This will save the data field to the S3 bucket set during initialization. The data must be a JSON compliant python object."
        try:
            content = bytes(json.dumps(data).encode('UTF-8'))
        except Exception as e:
            logging.error("The data does not seem to be JSON compliant.")
            raise e
        try:
            self.bucket.put_object(Key=filename, Body=content)
        except Exception as e:
            logging.error("Something went wrong while uploading to S3!")
            raise e

    def load_from_s3(self, filename):
        "Retrieves a JSON formated content from the S3 bucket"
        try:
            result = self.s3.get_object(Bucket=self.bucket_name, Key=filename)
        except Exception as e:
            logging.error("An error occured while retrieving data from S3!")
            raise e
        data = json.loads(result["Body"].read().decode('UTF-8'))
        return data

    def check_if_file_exists(self, filename):
        "This checks if the filename to be saved already exists and raises an error if so."
        try:
            boto3.resource('s3').Object(self.bucket_name, filename).load()
        except ClientError as e:
            if e.response['Error']['Code'] != "404":
                logging.error("Something went wrong while checking if the data file already existed in the bucket!")
                raise e
            else:
                return False
        else:
            return True

    def create_or_get_bucket(self):
        response = self.s3.list_buckets()
        if self.bucket_name not in [el["Name"] for el in response["Buckets"]]:
            try:
                logging.warning("Bucket not found! Creating {}".format(self.bucket_name))
                location = {'LocationConstraint': os.environ['AWS_DEFAULT_REGION']}
                self.s3.create_bucket(Bucket=self.bucket_name, CreateBucketConfiguration=location)
            except ClientError as e:
                logging.error("An error occured during the creation of the bucket!")
                raise e
        return boto3.resource('s3').Bucket(self.bucket_name)

