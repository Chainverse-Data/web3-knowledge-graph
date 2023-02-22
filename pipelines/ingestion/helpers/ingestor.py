from multiprocessing.sharedctypes import Value
import os
import logging
from datetime import datetime
import re
import sys

from ...helpers import Base

class Ingestor(Base):
    def __init__(self, bucket_name, start_date=None, end_date=None):
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

        self.start_date = start_date
        self.end_date = end_date
        self.set_start_end_date()

        self.scraper_data = {}
        self.load_data()

        self.ingest_data = {}

    def run(self):
        "Main function to be called. Every ingestor must implement its own run function!"
        raise NotImplementedError("ERROR: the run function has not been implemented!")

    def set_start_end_date(self):
        "Sets the start and end date from either params, env or metadata"
        if not self.start_date and "INGEST_FROM_DATE" in os.environ and os.environ["INGEST_FROM_DATE"].strip():
            self.start_date = os.environ["INGEST_FROM_DATE"]
        else:
            if "last_date_ingested" in self.metadata:
                self.start_date = self.metadata["last_date_ingested"]
        if not self.end_date and "INGEST_TO_DATE" in os.environ and os.environ["INGEST_TO_DATE"].strip():
            self.end_date = os.environ["INGEST_TO_DATE"]
        # Converting to python datetime object for easy filtering
        if self.start_date:
            self.start_date = datetime.strptime(self.start_date, "%Y-%m-%d")
        if self.end_date:
            self.end_date = datetime.strptime(self.end_date, "%Y-%m-%d")