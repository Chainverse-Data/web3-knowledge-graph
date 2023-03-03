import logging
import time
import requests
from datetime import datetime
import os

from ...helpers import S3Utils
from ...helpers import Requests
from ...helpers import Multiprocessing

class Processor(Requests, S3Utils, Multiprocessing):
    def __init__(self, bucket_name):
        Requests.__init__(self)
        S3Utils.__init__(self)
        Multiprocessing.__init__(self)
        self.runtime = datetime.now()
        self.asOf = f"{self.runtime.year}-{self.runtime.month}-{self.runtime.day}"
        try:
            self.cyphers
        except:
            raise ValueError("Cyphers have not been instanciated to self.cyphers")
        # if bucket_name:
            # raise ValueError("bucket_name is not defined!")
        self.bucket_name = os.environ["AWS_BUCKET_PREFIX"] + bucket_name
        self.bucket = self.create_or_get_bucket(self.bucket_name)

        self.metadata_filename = "processor_metadata.json"
        self.metadata = self.read_metadata()

    def read_metadata(self):
        "Access the S3 bucket to read the metadata and returns a dictionary that corresponds to the saved JSON object"
        if self.check_if_file_exists(self.bucket_name, self.metadata_filename):
            return self.load_json(self.bucket_name, self.metadata_filename)
        else:
            return {}

    def save_metadata(self):
        "Saves the current metadata to S3"
        self.save_json(self.bucket_name,
                          self.metadata_filename, self.metadata)

    def run(self):
        "Main function to be called. Every postprocessor must implement its own run function!"
        raise NotImplementedError(
            "ERROR: the run function has not been implemented!")
