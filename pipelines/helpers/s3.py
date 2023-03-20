from datetime import datetime
import math
import re
import sys
import boto3
import os
import logging
import json
from botocore.exceptions import ClientError
import pandas as pd

class S3Utils:
    def __init__(self, bucket_name, metadata_filename, load_bucket_data=True):
        self.s3_client = boto3.client("s3")
        self.s3_resource = boto3.resource("s3")
        self.S3_max_size = 1000000000
        
        self.scraper_data = {}
        
        if not bucket_name:
            raise ValueError("bucket_name is not defined!")
        self.bucket_name = os.environ["AWS_BUCKET_PREFIX"] + bucket_name
        self.bucket = self.create_or_get_bucket(self.bucket_name)
        
        if not metadata_filename:
            raise ValueError("bucket_name is not defined!")
        self.metadata_filename = metadata_filename
        self.metadata = self.read_metadata()

        self.start_date = None
        self.end_date = None
        self.set_start_end_date()
        
        if load_bucket_data:
            self.scraper_data = {}
            self.load_data()

        if "ALLOW_OVERRIDE" in os.environ and os.environ["ALLOW_OVERRIDE"] == "1":
            allow_override = True

        self.data = {}
        self.data_filename = "data_{}-{}-{}".format(self.runtime.year, self.runtime.month, self.runtime.day)
        if not allow_override and self.check_if_file_exists(self.bucket_name, self.data_filename):
            logging.error("The data file for this day has already been created!")
            sys.exit(0)

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

    def get_size(self, obj, seen=None):
        """Recursively finds size of objects"""
        size = sys.getsizeof(obj)
        if seen is None:
            seen = set()
        obj_id = id(obj)
        if obj_id in seen:
            return 0
        # Important mark as seen *before* entering recursion to gracefully handle
        # self-referential objects
        seen.add(obj_id)
        if isinstance(obj, dict):
            size += sum([self.get_size(v, seen) for v in obj.values()])
            size += sum([self.get_size(k, seen) for k in obj.keys()])
        elif hasattr(obj, '__dict__'):
            size += self.get_size(obj.__dict__, seen)
        elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
            size += sum([self.get_size(i, seen) for i in obj])
        return size

    def save_json(self, bucket_name, filename, data):
        "This will save the data field to the S3 bucket set during initialization. The data must be a JSON compliant python object."
        try:
            content = bytes(json.dumps(data).encode("UTF-8"))
        except Exception as e:
            logging.error("The data does not seem to be JSON compliant.")
            raise e
        try:
            self.s3_client.put_object(Bucket=bucket_name, Key=filename, Body=content)
        except Exception as e:
            logging.error("Something went wrong while uploading to S3!")
            raise e

    def save_file(self, bucket_name, local_path, s3_path):
        "This will save the data field to the S3 bucket set during initialization. The data must be a JSON compliant python object."
        try:
            self.s3_client.upload_file(local_path, bucket_name, s3_path)
        except Exception as e:
            logging.error("Something went wrong while uploading to S3!")
            raise e
            
    def save_df_as_csv(self, df, bucket_name, file_name, ACL='public-read'):
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


    def save_df_as_csv(self, df, bucket_name, file_name, ACL='public-read', max_lines=10000, max_size=10000000):
        """Function to save a Pandas DataFrame to a CSV file in S3.
        This functions takes care of splitting the dataframe if the resulting CSV is more than 10Mb.
        parameters:
        - df: the dataframe to be saved.
        - bucket_name: The bucket name.
        - file_name: The file name (without .csv at the end).
        - ACL: (Optional) defaults to public-read for neo4J ingestion."""
        chunks = [df]
        # Check if the dataframe is bigger than the max allowed size of Neo4J (10Mb)
        if df.memory_usage(index=False).sum() > max_size or len(df) > max_lines:
            chunks = self.split_dataframe(df, chunk_size=max_lines)

        logging.info("Uploading data...")
        urls = []
        for chunk, chunk_id in zip(chunks, range(len(chunks))):
            chunk.to_csv(f"s3://{bucket_name}/{file_name}--{chunk_id}.csv", index=False, escapechar='\\')
            self.s3_resource.ObjectAcl(bucket_name, f"{file_name}--{chunk_id}.csv").put(ACL=ACL)
            location = self.s3_client.get_bucket_location(Bucket=bucket_name)["LocationConstraint"]
            urls.append("https://s3-%s.amazonaws.com/%s/%s" % (location, bucket_name, f"{file_name}--{chunk_id}.csv"))
        return urls

    def save_json_as_csv(self, data, bucket_name, file_name, ACL="public-read", max_lines=10000, max_size=10000000):
        """Function to save a python list of dictionaries (json compatible) to a CSV in S3.
        This functions takes care of splitting the array if the resulting CSV is more than 10Mb.
        parameters:
        - data: the data array.
        - bucket_name: The bucket name
        - file_name: The file name (without .csv at the end)
        - ACL: (Optional) defaults to public-read for neo4J ingestion."""
        df = pd.DataFrame.from_dict(data)
        return self.save_df_as_csv(df, bucket_name, file_name, ACL=ACL, max_lines=max_lines, max_size=max_size)

    def save_full_json_as_csv(self, data, bucket_name, file_name, ACL="public-read"):
        df = pd.DataFrame.from_dict(data)
        df.to_csv(f"s3://{bucket_name}/{file_name}.csv", index=False)
        self.s3_resource.ObjectAcl(bucket_name, f"{file_name}.csv").put(ACL=ACL)
        location = self.s3_client.get_bucket_location(Bucket=bucket_name)["LocationConstraint"]
        url = "https://s3-%s.amazonaws.com/%s/%s" % (location, bucket_name, f"{file_name}.csv")
        return url

    def load_csv(self, bucket_name, file_name):
        """Convenience function to retrieve a S3 saved CSV loaded as a pandas dataframe."""
        try:
            df = pd.read_csv(f"s3://{bucket_name}/{file_name}", lineterminator="\n")
            return df
        except:
            return None

    def split_dataframe(self, df, chunk_size=10000):
        chunks = list()
        num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size else 0)
        for i in range(num_chunks):
            chunks.append(df[i * chunk_size : (i + 1) * chunk_size])
        return chunks

    def load_json(self, bucket_name, filename):
        "Retrieves a JSON formated content from the S3 bucket"
        try:
            result = self.s3_client.get_object(Bucket=bucket_name, Key=filename)
        except Exception as e:
            logging.error("An error occured while retrieving data from S3!")
            raise e
        data = json.loads(result["Body"].read().decode("UTF-8"))
        return data

    def check_if_file_exists(self, bucket_name, filename):
        "This checks if the filename to be saved already exists and raises an error if so."
        try:
            boto3.resource("s3").Object(bucket_name, filename).load()
        except ClientError as e:
            if e.response["Error"]["Code"] != "404":
                logging.error("Something went wrong while checking if the data file already existed in the bucket!")
                raise e
            else:
                return False
        else:
            return True

    def create_or_get_bucket(self, bucket_name):
        response = self.s3_client.list_buckets()
        if bucket_name not in [el["Name"] for el in response["Buckets"]]:
            try:
                logging.warning("Bucket not found! Creating {}".format(bucket_name))
                location = {"LocationConstraint": os.environ["AWS_DEFAULT_REGION"]}
                self.s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
                logging.info(f"Creating bucket: {bucket_name}")
            except ClientError as e:
                logging.error(f"An error occured during the creation of the bucket: {bucket_name}")
                raise e
        else:
            logging.info(f"Using existing bucket: {bucket_name}")
        return boto3.resource("s3").Bucket(bucket_name)

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

    def save_data(self, chunk_prefix=""):
        "Saves the current data to S3. This will take care of chunking the data to less than 5Gb for AWS S3 requierements."
        logging.info("Saving the results to S3 ...")
        logging.info("Measuring data size...")
        data_size = self.get_size(self.data)
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

    def get_datafile_from_s3(self):
        "Get the list of datafiles in the S3 bucket from the start date to the end date (if defined)"
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
                if self.end_date and date >= self.end_date:
                    break
                datafiles_to_keep.append(datafile)
                dates_to_keep.append(date)
        if len(dates_to_keep) == 0:
            logging.error("No data file found that match the current date range")
            sys.exit(1)
        if not self.end_date:
            self.end_date = max(dates_to_keep)
        logging.info("Datafiles for ingestion: {}".format(",".join(datafiles_to_keep)))
        return datafiles_to_keep

    def load_data(self):
        "Loads the data filtered by date saved in the S3 bucket"
        datafiles_to_keep = self.get_datafile_from_s3()
        logging.info("Datafiles for ingestion: {}".format(",".join(datafiles_to_keep)))
        for datafile in datafiles_to_keep:
            tmp_data = self.load_json(self.bucket_name, datafile)
            for root_key in tmp_data:
                if root_key not in self.scraper_data:
                    self.scraper_data[root_key] = type(tmp_data[root_key])()
                if type(tmp_data[root_key]) == dict:
                    self.scraper_data[root_key] = dict(self.scraper_data[root_key], **tmp_data[root_key])
                if type(tmp_data[root_key]) == list:
                    self.scraper_data[root_key] += tmp_data[root_key]
        logging.info("Data files loaded")

    def load_data_iterate(self, nb_files=1):
        "Generator function to load the datafiles one file at a time. Returns the content of N datafile at a time, N being the nb_files parameter."
        datafiles_to_keep = self.get_datafile_from_s3()
        counter = 0
        data = {}
        for datafile in datafiles_to_keep:
            logging.info(f"Loading datafile: {datafile}")
            tmp_data = self.load_json(self.bucket_name, datafile)
            for root_key in tmp_data:
                if root_key not in data:
                    data[root_key] = type(tmp_data[root_key])()
                if type(tmp_data[root_key]) == dict:
                    data[root_key] = dict(data[root_key], **tmp_data[root_key])
                if type(tmp_data[root_key]) == list:
                    data[root_key] += tmp_data[root_key]
            counter += 1
            if counter >= nb_files:
                yield data
                data = {}
                counter = 0
