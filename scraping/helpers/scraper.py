import requests
import datetime
import logging
import os
from helpers import S3Utils 
import time

# This class is the base class for all scrapers.
# Every scraper must inherit this class and define its own run function
# The base class takes care of loading the metadata and checking that data was not already ingested for today
# The data and metadata are not saved automatically at the end of the execution to allow for user defined save points.
# Use the save_metadata and save_data functions to automatically save to S3
# During the run function, save the data into the instance.data field. 
# The data field is a dictionary, define each root key as you would a mongoDB index.
 
class Scraper:
    def __init__(self, bucket_name, allow_override=None):
        self.runtime = datetime.datetime.now()

        if not bucket_name:
            raise ValueError("bucket_name is not defined!")
        self.bucket_name = os.environ["AWS_BUCKET_PREFIX"] + bucket_name
        
        if not allow_override and "ALLOW_OVERRIDE" in os.environ and int(os.environ["ALLOW_OVERRIDE"]) == 1:
            allow_override = True
        
        self.s3 = S3Utils()
        self.bucket = self.s3.create_or_get_bucket(self.bucket_name)
        
        self.data = {}
        self.data_filename = "data_{}-{}-{}.json".format(self.runtime.year, self.runtime.month, self.runtime.day)
        if not allow_override and self.s3.check_if_file_exists(self.bucket_name, self.data_filename):
            logging.error("The data file for this day has already been created!")
            raise Exception("The data file for this day has already been created!")
        
        self.metadata_filename = "scraper_metadata.json"
        self.metadata = self.read_metadata()

    def run(self):
        "Main function to be called. Every scrapper must implement its own run function !"
        raise NotImplementedError("ERROR: the run function has not been implemented!")

    # This section handles requests and networking.

    def get_request(self, url, params=None, headers=None, counter=0):
        """This makes a GET request to a url and return the data. 
        It can take params and headers as parameters following python's request library.
        The method returns the raw request content, you must then parse the content with the correct parser."""
        time.sleep(counter * 10)
        if counter > 10:
            return None
        r = requests.get(url, params=params, headers=headers)
        if r.status_code != 200:
            logging.error(f"Status code not 200: {r.status_code} Retrying in {counter*10}s (counter = {counter})...")
            return self.get_request(url, params=params, headers=headers, counter=counter+1)
        if "403 Forbidden" in r.content.decode('UTF-8'):
            logging.error(f"Status code not 200: {r.status_code} Retrying in {counter*10}s (counter = {counter})...")
            return self.get_request(url, params=params, headers=headers, counter=counter+1)
        return r.content.decode('UTF-8')

    def post_request(self, url, data=None, json=None, headers=None, counter=0):
        time.sleep(counter * 10)
        if counter > 10:
            return None
        r = requests.post(url, data=data, json=json, headers=headers)
        if r.status_code <= 200 and r.status_code > 300:
            logging.error(f"Status code not 200: {r.status_code} Retrying {counter*10}s (counter = {counter})...")
            return self.post_request(url, data=data, json=json, headers=headers, counter=counter+1)
        return r.content.decode('UTF-8')

    # This section contains functions to deal with S3 storage.

    def read_metadata(self):
        "Access the S3 bucket to read the metadata and returns a dictionary that corresponds to the saved JSON object"
        if "REINITIALIZE" in os.environ and os.environ["REINITIALIZE"] == "1":
            return {}
        if self.s3.check_if_file_exists(self.bucket_name, self.metadata_filename):
            return self.s3.load_json(self.bucket_name, self.metadata_filename)
        else:
            return {}
        
    def save_metadata(self):
        "Saves the current metadata to S3"
        self.s3.save_json(self.bucket_name, self.metadata_filename, self.metadata)

    def save_data(self):
        "Saves the current data to S3"
        self.s3.save_json(self.bucket_name, self.data_filename, self.data)