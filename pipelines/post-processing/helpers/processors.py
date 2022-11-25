from ...helpers import Cypher
from datetime import datetime
import os
from ...helpers import S3Utils

class Processor():
    def __init__(self, bucket_name):
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

        self.metadata_filename = "processor_metadata.json"
        self.metadata = self.read_metadata()

    def read_metadata(self):
        "Access the S3 bucket to read the metadata and returns a dictionary that corresponds to the saved JSON object"
        if self.s3.check_if_file_exists(self.bucket_name, self.metadata_filename):
            return self.s3.load_json(self.bucket_name, self.metadata_filename)
        else:
            return {}

    def save_metadata(self):
        "Saves the current metadata to S3"
        self.s3.save_json(self.bucket_name,
                          self.metadata_filename, self.metadata)


    def run(self):
        "Main function to be called. Every postprocessor must implement its own run function!"
        raise NotImplementedError(
            "ERROR: the run function has not been implemented!")
