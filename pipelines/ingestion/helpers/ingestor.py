from multiprocessing.sharedctypes import Value
import os
import logging
from datetime import datetime
import re
import sys

from ...helpers import Base

class Ingestor(Base):
    def __init__(self, bucket_name):
        Base.__init__(self)
        self.runtime = datetime.now()
        self.asOf = f"{self.runtime.year}-{self.runtime.month}-{self.runtime.day}"

        try:
            self.cyphers
        except:
            raise NotImplementedError("Cyphers have not been instanciated to self.cyphers")

        if not bucket_name:
            raise ValueError("bucket_name is not defined!")
        self.bucket_name = os.environ["AWS_BUCKET_PREFIX"] + bucket_name

        self.bucket = self.create_or_get_bucket(self.bucket_name)

        self.metadata_filename = "ingestor_metadata.json"
        self.metadata = self.read_metadata()

        self.scraper_data = {}
        self.load_data()

        self.ingest_data = {}

    def run(self):
        "Main function to be called. Every ingestor must implement its own run function!"
        raise NotImplementedError("ERROR: the run function has not been implemented!")

    def save_metadata(self):
        "Saves the current metadata to S3"
        self.metadata["last_date_ingested"] = f"{self.runtime.year}-{self.runtime.month}-{self.runtime.day}"
        self.s3.save_json(self.bucket_name, self.metadata_filename, self.metadata)