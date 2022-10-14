import chunk
import boto3
import os
import logging
import json
import boto3
from botocore.exceptions import ClientError
import pandas as pd

class S3Utils:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')

    def save_json(self, bucket_name, filename, data):
        "This will save the data field to the S3 bucket set during initialization. The data must be a JSON compliant python object."
        try:
            content = bytes(json.dumps(data).encode('UTF-8'))
        except Exception as e:
            logging.error("The data does not seem to be JSON compliant.")
            raise e
        try:
            self.s3_client.put_object(Bucket=bucket_name, Key=filename, Body=content)
        except Exception as e:
            logging.error("Something went wrong while uploading to S3!")
            raise e

    def save_json_as_csv(self, data, bucket_name, file_name, ACL="public-read"):
        """Function to save a python list of dictionaries (json compatible) to a CSV in S3.
        This functions takes care of splitting the array if the resulting CSV is more than 10Mb.
        parameters:
        - data: the data array.
        - bucket_name: The bucket name
        - file_name: The file name (without .csv at the end)
        - ACL: (Optional) defaults to public-read for neo4J ingestion."""
        df = pd.DataFrame.from_dict(data)
        chunks = [df]
        # Check if the dataframe is bigger than the max allowed size of Neo4J (10Mb)
        if df.memory_usage(index=False).sum() > 10000000:
            chunks = self.split_dataframe(df)

        urls = []
        for chunk, chunk_id in zip(chunks, range(len(chunks))):
            chunk.to_csv(f"s3://{bucket_name}/{file_name}--{chunk_id}.csv", index=False)
            self.s3_resource.ObjectAcl(
                bucket_name, f"{file_name}--{chunk_id}.csv").put(ACL=ACL)
            location = self.s3_client.get_bucket_location(Bucket=bucket_name)["LocationConstraint"]
            urls.append("https://s3-%s.amazonaws.com/%s/%s" %
                        (location, bucket_name, f"{file_name}--{chunk_id}.csv"))
        return urls

    def load_csv(self, bucket_name, file_name):
        """Convenience function to retrieve a S3 saved CSV loaded as a pandas dataframe."""
        df = pd.read_csv(f"s3://{bucket_name}/{file_name}", lineterminator='\n')
        return df

    # def set_object_private(BUCKET, file_name, resource):
    #     object_acl = resource.ObjectAcl(BUCKET, file_name)
    #     response = object_acl.put(ACL="private")


    def split_dataframe(self, df, chunk_size=10000):
        chunks = list()
        num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size else 0)
        for i in range(num_chunks):
            chunks.append(df[i * chunk_size: (i + 1) * chunk_size])
        return chunks

    def load_json(self, bucket_name, filename):
        "Retrieves a JSON formated content from the S3 bucket"
        try:
            result = self.s3_client.get_object(Bucket=bucket_name, Key=filename)
        except Exception as e:
            logging.error("An error occured while retrieving data from S3!")
            raise e
        data = json.loads(result["Body"].read().decode('UTF-8'))
        return data

    def check_if_file_exists(self, bucket_name, filename):
        "This checks if the filename to be saved already exists and raises an error if so."
        try:
            boto3.resource('s3').Object(bucket_name, filename).load()
        except ClientError as e:
            if e.response['Error']['Code'] != "404":
                logging.error(
                    "Something went wrong while checking if the data file already existed in the bucket!")
                raise e
            else:
                return False
        else:
            return True

    def create_or_get_bucket(self, bucket_name):
        print(f"Creating bucket: {bucket_name}")
        response = self.s3_client.list_buckets()
        if bucket_name not in [el["Name"] for el in response["Buckets"]]:
            try:
                logging.warning(
                    "Bucket not found! Creating {}".format(bucket_name))
                location = {
                    'LocationConstraint': os.environ['AWS_DEFAULT_REGION']}
                self.s3_client.create_bucket(
                    Bucket=bucket_name, CreateBucketConfiguration=location)
            except ClientError as e:
                logging.error(
                    "An error occured during the creation of the bucket!")
                raise e
        return boto3.resource('s3').Bucket(bucket_name)
