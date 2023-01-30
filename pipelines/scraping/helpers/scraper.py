import math
import sys
import requests
from datetime import datetime
import logging
import os
from ...helpers import S3Utils
import time
import urllib3
from .utils import get_size
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# This class is the base class for all scrapers.
# Every scraper must inherit this class and define its own run function
# The base class takes care of loading the metadata and checking that data was not already ingested for today
# The data and metadata are not saved automatically at the end of the execution to allow for user defined save points.
# Use the save_metadata and save_data functions to automatically save to S3
# During the run function, save the data into the instance.data field.
# The data field is a dictionary, define each root key as you would a mongoDB index.


class Scraper:
    def __init__(self, bucket_name, allow_override=None):
        self.runtime = datetime.now()

        if not bucket_name:
            raise ValueError("bucket_name is not defined!")
        self.bucket_name = os.environ["AWS_BUCKET_PREFIX"] + bucket_name

        if not allow_override and "ALLOW_OVERRIDE" in os.environ and os.environ["ALLOW_OVERRIDE"] == "1":
            allow_override = True

        self.s3 = S3Utils()
        self.bucket = self.s3.create_or_get_bucket(self.bucket_name)
        # It is hard to estimate exactly the size of an object without exploding python's memory
        # A good rule of thumb is that this number should be 3x the file size
        # for a 1Mb file size, it should be 3000000
        # for 1Gb: 3000000000
        # AWS max file size with a PUT uploaod is 5Gb so to be on the safe side: 10000000000 (around 3.5Gb)
        self.S3_max_size = 1000000000

        self.data = {}
        self.data_filename = "data_{}-{}-{}".format(self.runtime.year, self.runtime.month, self.runtime.day)
        if not allow_override and self.s3.check_if_file_exists(self.bucket_name, self.data_filename):
            logging.error("The data file for this day has already been created!")
            sys.exit(0)

        self.metadata_filename = "scraper_metadata.json"
        self.metadata = self.read_metadata()

        self.isAirflow = os.environ.get("IS_AIRFLOW", False)

    def run(self):
        "Main function to be called. Every scrapper must implement its own run function !"
        raise NotImplementedError("ERROR: the run function has not been implemented!")

    # This section handles requests and networking.

    def get_request(self, url, params=None, headers=None, allow_redirects=True, decode=True, json=False, counter=0):
        """This makes a GET request to a url and return the data.
        It can take params and headers as parameters following python's request library.
        The method returns the raw request content, you must then parse the content with the correct parser."""
        time.sleep(counter * 10)
        if counter > 10:
            return None
        try:
            r = requests.get(url, params=params, headers=headers,
                            allow_redirects=allow_redirects, verify=False)
            if r.status_code != 200:
                logging.error(f"Status code not 200: {r.status_code} Retrying in {counter*10}s (counter = {counter})...")
                return self.get_request(url, params=params, headers=headers, allow_redirects=allow_redirects, counter=counter + 1)
            if "403 Forbidden" in r.content.decode("UTF-8"):
                logging.error(f"Status code not 200: {r.status_code} Retrying in {counter*10}s (counter = {counter})...")
                return self.get_request(url, params=params, headers=headers, allow_redirects=allow_redirects, counter=counter + 1)
            if decode:
                return r.content.decode("UTF-8")
            if json:
                return r.json()
            return r
        except Exception as e:
            logging.error(f"An unrecoverable exception occurred: {e}")
            return self.get_request(url, params=params, headers=headers, allow_redirects=allow_redirects, counter=counter + 1)

    def post_request(self, url, data=None, json=None, headers=None, counter=0):
        time.sleep(counter * 10)
        if counter > 10:
            return None
        try:
            r = requests.post(url, data=data, json=json, headers=headers, verify=False)
            if r.status_code <= 200 and r.status_code > 300:
                logging.error(f"Status code not 200: {r.status_code} Retrying {counter*10}s (counter = {counter})...")
                return self.post_request(url, data=data, json=json, headers=headers, counter=counter + 1)
            return r.content.decode("UTF-8")
        except Exception as e:
            logging.error(f"An unrecoverable exception occurred: {e}")
            return self.post_request(url, data=data, json=json, headers=headers, counter=counter + 1)

    # This section contains functions to deal with S3 storage.

    def read_metadata(self):
        "Access the S3 bucket to read the metadata and returns a dictionary that corresponds to the saved JSON object"
        logging.info("Loading the metadata from S3 ...")
        if "REINITIALIZE" in os.environ and os.environ["REINITIALIZE"] == "1":
            return {}
        if self.s3.check_if_file_exists(self.bucket_name, self.metadata_filename):
            return self.s3.load_json(self.bucket_name, self.metadata_filename)
        else:
            return {}

    def save_metadata(self):
        "Saves the current metadata to S3"
        logging.info("Saving the metadata to S3 ...")
        self.s3.save_json(self.bucket_name, self.metadata_filename, self.metadata)

    def save_data(self, chunk_prefix=""):
        "Saves the current data to S3. This will take care of chuking the data to less than 5Gb for AWS S3 requierements."
        logging.info("Saving the results to S3 ...")
        logging.info("Measuring data size...")
        data_size = get_size(self.data)
        logging.info(f"Data size: {data_size}")
        if data_size > self.S3_max_size:
            n_chunks = math.ceil(data_size / self.S3_max_size)
            logging.info(f"Data is too big: {data_size}, chuking it to {n_chunks} chunks ...")
            len_data = {}
            for key in self.data:
                len_data[key] = math.ceil(len(self.data[key])/n_chunks)
            for i in range(n_chunks):
                data_chunk = {}
                for key in self.data:
                    if type(self.data[key]) == dict:
                        data_chunk[key] = {}
                        chunk_keys = list(self.data[key].keys())[i*len_data[key]:min((i+1)*len_data[key], len(self.data[key]))]
                        for chunk_key in chunk_keys:
                            data_chunk[key][chunk_key] = self.data[key][chunk_key]
                    else:
                        data_chunk[key] = self.data[key][i*len_data[key]:min((i+1)*len_data[key], len(self.data[key]))]
                filename = self.data_filename + f"_{chunk_prefix}{i}.json"
                logging.info(f"Saving chunk {i}...")
                self.s3.save_json(self.bucket_name, filename, data_chunk)
        else:
            self.s3.save_json(self.bucket_name, self.data_filename + f"_{chunk_prefix}.json", self.data)
