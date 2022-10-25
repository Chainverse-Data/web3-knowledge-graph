from datetime import datetime
import os
from ...helpers import S3Utils

class Analysis():
    def __init__(self, bucket_name):
        self.runtime = datetime.now()
        self.asOf = f"{self.runtime.year}-{self.runtime.month}-{self.runtime.day}"

        if not bucket_name:
            raise ValueError("bucket_name is not defined!")
        self.bucket_name = os.environ["AWS_BUCKET_PREFIX"] + bucket_name

        self.s3 = S3Utils()
        self.bucket = self.s3.create_or_get_bucket(self.bucket_name)

    def run(self):
        "Main function to be called. Every analytics must implement its own run function!"
        raise NotImplementedError(
            "ERROR: the run function has not been implemented!")
