import requests
import boto3

class Scraper:
    def __init__(self, bucket):
        self.data = {}
        self.bucket = bucket

    def get_request(self, url, params=None, header=None, counter=0):
        "This makes a GET request to a url and return the data. It can take params and header as parameters following python's request library"
        if counter > 10:
            return None
        r = requests.get(url, params=params, header=header)
        if r.status_code != 200:
            self.get_request(url, params=params, header=header, counter=counter+1)
        return r.content
    
    def save_to_s3(self):
        "This will save the scraped content to the S3 bucket set during initialization."
        pass

    def run(self):
        "Main function to be called. Every scrapper must implement its own run function !"
        raise NotImplementedError("ERROR: the run function has not been implemented!")
