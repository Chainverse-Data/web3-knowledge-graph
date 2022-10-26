from multiprocessing.sharedctypes import Value
import os
import logging
from datetime import datetime

from ...helpers.s3 import S3Utils


class Ingestor:
    def __init__(self, bucket_name, start_date=None, end_date=None):
        self.runtime = datetime.now()
        self.asOf = f"{self.runtime.year}-{self.runtime.month}-{self.runtime.day}"

        try:
            self.cyphers
        except:
            raise ValueError("Cyphers have not been instanciated to self.cyphers")

        if not bucket_name:
            raise ValueError("bucket_name is not defined!")
        self.bucket_name = os.environ["AWS_BUCKET_PREFIX"] + bucket_name

        self.s3 = S3Utils()
        self.bucket = self.s3.create_or_get_bucket(self.bucket_name)

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
        if not self.start_date and "INGEST_FROM_DATE" in os.environ:
            self.start_date = os.environ["INGEST_FROM_DATE"]
        else:
            if "last_date_ingested" in self.metadata:
                self.start_date = self.metadata["last_date_ingested"]
        if not self.end_date and "INGEST_TO_DATE" in os.environ:
            self.end_date = os.environ["INGEST_TO_DATE"]
        # Converting to python datetime object for easy filtering
        if self.start_date:
            self.start_date = datetime.strptime(self.start_date, "%Y-%m-%d")
        if self.end_date:
            self.end_date = datetime.strptime(self.end_date, "%Y-%m-%d")
            

    def read_metadata(self):
        "Access the S3 bucket to read the metadata and returns a dictionary that corresponds to the saved JSON object"
        if self.s3.check_if_file_exists(self.bucket_name, self.metadata_filename):
            return self.s3.load_json(self.bucket_name, self.metadata_filename)
        else:
            return {}

    def save_metadata(self):
        "Saves the current metadata to S3"
        self.s3.save_json(self.bucket_name, self.metadata_filename, self.metadata)

    def load_data(self):
        "Loads the data in the S3 bucket from the start date to the end date (if defined)"
        logging.info("Collecting data files")
        datafiles = []
        for el in map(lambda x: (x.bucket_name, x.key), self.bucket.objects.all()):
            if "data_" in el[1]:
                datafiles.append(el[1])
        dates = [datetime.strptime(key, "data_%Y-%m-%d.json") for key in datafiles]
        datafiles_to_keep = []
        dates_to_keep = []
        for datafile, date in sorted(zip(datafiles, dates), key=lambda el: el[1]):
            if not self.start_date:
                self.start_date = date
            if date >= self.start_date:
                if self.end_date and date <= self.end_date:
                    break
                datafiles_to_keep.append(datafile)
                dates_to_keep.append(date)
        if not self.end_date:
            self.end_date = max(dates_to_keep)
        logging.info("Datafiles for ingestion: {}".format(",".join(datafiles_to_keep)))
        for datafile in datafiles_to_keep:
            tmp_data = self.s3.load_json(self.bucket_name, datafile)
            for root_key in tmp_data:
                if root_key not in self.scraper_data:
                    self.scraper_data[root_key] = type(tmp_data[root_key])()
                if type(tmp_data[root_key]) == dict:
                    self.scraper_data[root_key] = dict(self.scraper_data[root_key], **tmp_data[root_key])
                if type(tmp_data[root_key]) == list:
                    self.scraper_data[root_key] += tmp_data[root_key]
        logging.info("Data files loaded")