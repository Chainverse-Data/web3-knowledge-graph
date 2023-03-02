import math
import sys
import requests
from datetime import datetime
import logging
import os
from ...helpers import Requests
from ...helpers import S3Utils
from ...helpers import Multiprocessing
import time
from .utils import get_size

# This class is the base class for all scrapers.
# Every scraper must inherit this class and define its own run function
# The base class takes care of loading the metadata and checking that data was not already ingested for today
# The data and metadata are not saved automatically at the end of the execution to allow for user defined save points.
# Use the save_metadata and save_data functions to automatically save to S3
# During the run function, save the data into the instance.data field.
# The data field is a dictionary, define each root key as you would a mongoDB index.


class Scraper(Requests, S3Utils, Multiprocessing):
    def __init__(self, bucket_name, allow_override=None):
        Requests.__init__(self)
        S3Utils.__init__(self)
        Multiprocessing.__init__(self)
        self.runtime = datetime.now()

        if not bucket_name:
            raise ValueError("bucket_name is not defined!")
        self.bucket_name = os.environ["AWS_BUCKET_PREFIX"] + bucket_name

        if not allow_override and "ALLOW_OVERRIDE" in os.environ and os.environ["ALLOW_OVERRIDE"] == "1":
            allow_override = True

        self.bucket = self.create_or_get_bucket(self.bucket_name)
        # It is hard to estimate exactly the size of an object without exploding python's memory
        # A good rule of thumb is that this number should be 3x the file size
        # for a 1Mb file size, it should be 3000000
        # for 1Gb: 3000000000
        # AWS max file size with a PUT uploaod is 5Gb so to be on the safe side: 10000000000 (around 3.5Gb)
        self.S3_max_size = 1000000000

        self.data = {}
        self.data_filename = "data_{}-{}-{}".format(self.runtime.year, self.runtime.month, self.runtime.day)
        if not allow_override and self.check_if_file_exists(self.bucket_name, self.data_filename):
            logging.error("The data file for this day has already been created!")
            sys.exit(0)

        self.metadata_filename = "scraper_metadata.json"
        self.metadata = self.read_metadata()

        self.isAirflow = os.environ.get("IS_AIRFLOW", False)

    def run(self):
        "Main function to be called. Every scrapper must implement its own run function !"
        raise NotImplementedError("ERROR: the run function has not been implemented!")

    # This section contains functions to deal with S3 storage.

    def read_metadata(self):
        "Access the S3 bucket to read the metadata and returns a dictionary that corresponds to the saved JSON object"
        logging.info("Loading the metadata from S3 ...")
        if "REINITIALIZE" in os.environ and os.environ["REINITIALIZE"] == "1":
            return {}
        if self.check_if_file_exists(self.bucket_name, self.metadata_filename):
            return self.load_json(self.bucket_name, self.metadata_filename)
        else:
            return {}

    def save_metadata(self):
        "Saves the current metadata to S3"
        logging.info("Saving the metadata to S3 ...")
        self.save_json(self.bucket_name, self.metadata_filename, self.metadata)

    def save_data(self, chunk_prefix=0):
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
                self.save_json(self.bucket_name, filename, data_chunk)
        else:
            self.save_json(self.bucket_name, self.data_filename + f"_{chunk_prefix}.json", self.data)
