import logging
import time
import requests
from datetime import datetime
import os

from ...helpers import Base

class Processor(Base):
    def __init__(self, bucket_name):
        Base.__init__(self)
        self.runtime = datetime.now()
        self.asOf = f"{self.runtime.year}-{self.runtime.month}-{self.runtime.day}"
        try:
            self.cyphers
        except:
            raise ValueError("Cyphers have not been instanciated to self.cyphers")
        if not bucket_name:
            raise ValueError("bucket_name is not defined!")
        self.bucket_name = os.environ["AWS_BUCKET_PREFIX"] + bucket_name
        self.bucket = self.create_or_get_bucket(self.bucket_name)

        self.metadata_filename = "processor_metadata.json"
        self.metadata = self.read_metadata()

    def run(self):
        "Main function to be called. Every postprocessor must implement its own run function!"
        raise NotImplementedError(
            "ERROR: the run function has not been implemented!")
