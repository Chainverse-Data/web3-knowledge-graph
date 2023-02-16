import logging
import pandas as pd
from tqdm import tqdm
from ..helpers import Processor
from .cyphers import TwitterRelsCyphers
from ...helpers import S3Utils
from urllib.parse import urlparse, urlunparse
from datetime import datetime, timedelta
import os
import re
import requests as r
import datetime
import json
import time


class TwitterRelsProcessor(Processor):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""

    def __init__(self):
        self.cyphers = TwitterRelsCyphers()
        self.s3 = S3Utils()
        self.now = datetime.datetime.now()
        self.timestamp = self.now.strftime("%Y_%m_%d_%H%M%S")
        logging.info(self.timestamp)
        self.bucket = 'jordan-test-twitter-rels'
        super().__init__("twitter-rels")

    def extract_handles(self, val):
        matches = re.findall(r'@\w+', val)
        if not matches:
            return None 
        return [match.strip() for match in matches]

    def extract_accounts_from_bio(self):
        bios = self.cyphers.get_bios()
        bios = bios.dropna(subset=['bio'])
        bios['handles'] = bios['bio'].apply(self.extract_handles)
        bios = bios.dropna(subset=['handles'])
        len_bios = len(bios)
        exploded_bios = bios.explode('handles')
        logging.info(f"nice, you have {len_bios} rows")
        return exploded_bios 

    
    def ingest_stuff(self):
        bios = self.extract_accounts_from_bio()
        self.cyphers.ingest_references(bios) 

    def get_websites(self):
        websites_df = self.cyphers.get_twitter_websites()
        return websites_df

    def extract_website_data(self, url, counter=0):
        if counter > 10:
            return {"url": None, "domain": None, "original_url": url}
        time.sleep(counter * 10)
        try:
            response = r.head(url, allow_redirects=True, timeout=10)
            parsed_url = urlparse(response.url)
            url_string = urlunparse(parsed_url)
            hostname = parsed_url.hostname
            return {"url": url_string, "domain": hostname, "original_url": url}
        except Exception as e:
            return self.extract_website_data(url, counter=counter+1) 
    
    def create_csv(self, website_df):
        results_list = list()
        for index, row in tqdm(website_df.iterrows(), total=len(website_df)):
            userId = row[0]
            tco_link = row[1]
            website_data = self.extract_website_data(tco_link)
            row_dict = dict()
            url = website_data['url']
            original_url = website_data['original_url']
            domain = website_data['domain']
            if url:
                row_dict['userId'] = userId 
                row_dict['original_url'] = original_url 
                row_dict['domain'] = domain 
                row_dict['url'] = url 
                results_list.append(row_dict)
        results_df = pd.DataFrame(results_list)
        fname = "websites_" + self.asOf
        urls = self.s3.save_df_as_csv(results_df, bucket_name=self.bucket_name, file_name=fname, ACL='public-read', max_lines=10000, max_size=10000000)
        return urls 

    def get_websites(self):
        logging.info("Collecting websites....")
        websites = self.cyphers.get_twitter_websites()
        logging.info("Getting website data, saving to S3...")
        urls = self.create_csv(website_df=websites)
        logging.info("Damn. That took awhile. Got all of the websites though")
        return urls 

    def ingest_websites(self):
        s



        return None 


if __name__ == "__main__":
    processor = TwitterRelsProcessor()
    processor.run()


