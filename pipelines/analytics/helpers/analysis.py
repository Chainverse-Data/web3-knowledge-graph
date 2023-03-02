from datetime import datetime
import os

from ...helpers import S3Utils
from ...helpers import Requests
from ...helpers import Multiprocessing


class Analysis(S3Utils, Requests, Multiprocessing):
    def __init__(self, bucket_name):
        Requests.__init__(self)
        S3Utils.__init__(self)
        Multiprocessing.__init__(self)

        if not bucket_name:
            raise ValueError("bucket_name is not defined!")
        try:
            self.cyphers
        except:
            raise ValueError("Cyphers have not been instanciated to self.cyphers")

        self.runtime = datetime.now()
        self.asOf = f"{self.runtime.year}-{self.runtime.month}-{self.runtime.day}"
        self.bucket_name = os.environ["AWS_BUCKET_PREFIX"] + bucket_name
        self.bucket = self.create_or_get_bucket(self.bucket_name)

    def run(self):
        "Main function to be called. Every analytics must implement its own run function!"
        raise NotImplementedError(
            "ERROR: the run function has not been implemented!")
