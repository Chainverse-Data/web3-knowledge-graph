from multiprocessing.sharedctypes import Value
import os
import logging
from datetime import datetime
import re
import sys

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

    def is_zero_address(self, address):
        if int(address, 16) == 0:
            return True
        return False

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

    def read_metadata(self):
        "Access the S3 bucket to read the metadata and returns a dictionary that corresponds to the saved JSON object"
        if self.s3.check_if_file_exists(self.bucket_name, self.metadata_filename):
            return self.s3.load_json(self.bucket_name, self.metadata_filename)
        else:
            return {}
    
    def save_df_as_csv(self, df, bucket_name, file_name, ACL="public-read"):
        """Function to save a Pandas DataFrame to a CSV file in S3.
        This functions takes care of splitting the dataframe if the resulting CSV is more than 10Mb.
        parameters:
        - df: the dataframe to be saved.
        - bucket_name: The bucket name.
        - file_name: The file name (without .csv at the end).
        - ACL: (Optional) defaults to public-read for neo4J ingestion."""
        chunks = [df]
        # Check if the dataframe is bigger than the max allowed size of Neo4J (10Mb)
        if df.memory_usage(index=False).sum() > 10000000:
            chunks = self.split_dataframe(df)

        urls = []
        for chunk, chunk_id in zip(chunks, range(len(chunks))):
            chunk.to_csv(f"s3://{bucket_name}/{file_name}--{chunk_id}.csv", index=False)
            self.s3_resource.ObjectAcl(bucket_name, f"{file_name}--{chunk_id}.csv").put(ACL=ACL)
            location = self.s3_client.get_bucket_location(Bucket=bucket_name)["LocationConstraint"]
            urls.append("https://s3-%s.amazonaws.com/%s/%s" % (location, bucket_name, f"{file_name}--{chunk_id}.csv"))
        return urls

    def save_metadata(self):
        "Saves the current metadata to S3"
        self.metadata["last_date_ingested"] = f"{self.runtime.year}-{self.runtime.month}-{self.runtime.day}"
        self.s3.save_json(self.bucket_name, self.metadata_filename, self.metadata)

    def load_data(self):
        "Loads the data in the S3 bucket from the start date to the end date (if defined)"
        logging.info("Collecting data files")
        datafiles = []
        for el in map(lambda x: (x.bucket_name, x.key), self.bucket.objects.all()):
            if "data_" in el[1]:
                datafiles.append(el[1])
        get_date = re.compile("data_([0-9]*-[0-9]*-[0-9]*).*")
        dates = [datetime.strptime(get_date.match(key).group(1), "%Y-%m-%d") for key in datafiles]
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
        if len(dates_to_keep) == 0:
            logging.error("No data file found that match the current date range")
            sys.exit(1)
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
