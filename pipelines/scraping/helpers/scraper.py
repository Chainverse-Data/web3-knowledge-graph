import sys
from datetime import datetime
import logging
import os
from ...helpers import Base

# This class is the base class for all scrapers.
# Every scraper must inherit this class and define its own run function
# The base class takes care of loading the metadata and checking that data was not already ingested for today
# The data and metadata are not saved automatically at the end of the execution to allow for user defined save points.
# Use the save_metadata and save_data functions to automatically save to S3
# During the run function, save the data into the instance.data field.
# The data field is a dictionary, define each root key as you would a mongoDB index.

class Scraper(Base):
    def __init__(self, bucket_name, allow_override=None, chain="ethereum"):
        Base.__init__(self, chain=chain)
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
