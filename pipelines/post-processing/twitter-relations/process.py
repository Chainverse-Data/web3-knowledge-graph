import logging
import pandas as pd
from tqdm import tqdm
from ..helpers import Processor
from .cyphers import TwitterRelationsCyphers
from ...helpers import S3Utils
from urllib.parse import urlparse, urlunparse
from datetime import datetime, timedelta
import os
import re
import requests as r
import datetime
import json
import time


class TwitterRelationsProcessor(Processor):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""

    def __init__(self):
        self.cyphers = TwitterRelationsCyphers()
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
        bios = pd.DataFrame.from_dict(self.cyphers.get_bios())
        bios = bios.dropna(subset=['bio'])
        bios['handles'] = bios['bio'].apply(self.extract_handles)
        bios = bios.dropna(subset=['handles'])
        len_bios = len(bios)
        exploded_bios = bios.explode('handles')
        logging.info(f"nice, you have {len_bios} rows")

        return exploded_bios 
    
    def process_references(self):
        bios = self.extract_accounts_from_bio()
        urls = self.save_df_as_csv(bios, self.bucket_name, "processing_bios_handles_refs_" + self.asOf)
        print(urls)
        # self.cyphers.ingest_references(urls)
        return None 

    def extract_website_data(self, account, counter=0):
        if counter > 5:
            return None
        time.sleep(counter * 10)
        try:
            response = r.head(account["website"], allow_redirects=True, timeout=10)
            parsed_url = urlparse(response.url)
            url_string = urlunparse(parsed_url)
            hostname = parsed_url.hostname
            
            result = {
                    "userId": account["userId"],
                    "original_url": account["website"],
                    "domain": hostname,
                    "url": url_string
                }
            return result
        except Exception as e:
            return self.extract_website_data(account, counter=counter+1) 
    
    def process_websites(self):
        logging.info("Extracting urls and domains for websites")
        twitter_accounts = self.cyphers.get_twitter_websites()
        results = self.parallel_process(self.extract_website_data, twitter_accounts, "Extracting URLs and domains from twitter accounts bios")
        results = [result for result in results if result]
        fname = "websites_" + self.asOf
        urls = self.s3.save_json_as_csv(results, bucket_name=self.bucket_name, file_name=fname, ACL='public-read', max_lines=10000, max_size=10000000)
        
        logging.info("ingesting twitter / website / domain data")
        self.cyphers.create_domains(urls)
        self.cyphers.create_websites(urls)
        self.cyphers.link_websites_domains(urls)
        self.cyphers.link_twitter_websites(urls)

    def run(self):
        self.process_references()
        self.process_websites()

if __name__ == "__main__":
    processor = TwitterRelationsProcessor()
    processor.run()
