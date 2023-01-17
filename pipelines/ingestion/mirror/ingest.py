from distutils.filelist import findall
import boto3
from datetime import datetime, timedelta
import json
import pandas as pd
from dotenv import load_dotenv
import os
import sys
from pathlib import Path
import re

from tqdm import tqdm
from ..helpers import Ingestor
from .cyphers import MirrorCyphers

class MirrorIngestor(Ingestor):
    def __init__(self):
        self.cyphers = MirrorCyphers()
        super().__init__("mirror")

    def prepare_articles(self):
        for i in tqdm(range(len(self.scraper_data["articles"]))):
            self.scraper_data["articles"][i]["body"] = self.cyphers.sanitize_text(self.scraper_data["articles"][i]["body"])
            self.scraper_data["articles"][i]["title"] = self.cyphers.sanitize_text(self.scraper_data["articles"][i]["title"])

    def ingest_articles(self):
        self.prepare_articles()
        urls = self.s3.save_json_as_csv(self.scraper_data["articles"], self.bucket_name, f"ingestor_articles_{self.asOf}", max_lines=1000)
        self.cyphers.create_or_merge_articles(urls)

        authors = [{"address" : article["author"]} for article in self.scraper_data["articles"]]
        urls = self.s3.save_json_as_csv(authors, self.bucket_name, f"ingestor_articles_authors_{self.asOf}")
        self.cyphers.queries.create_wallets(urls)

    def ingest_twitter(self):
        urls = self.s3.save_json_as_csv(self.scraper_data["twitter_accounts"], self.bucket_name, f"ingestor_twitter_{self.asOf}")
        self.cyphers.create_or_merge_twitter(urls)
        self.cyphers.link_twitter_to_article(urls)

    def ingest_nfts(self):
        owners = [{"address": nft["owner"]} for nft in self.scraper_data["NFTs"]]
        urls = self.s3.save_json_as_csv(owners, self.bucket_name, f"ingestor_nfts_owners_{self.asOf}")
        self.cyphers.queries.create_wallets(urls)

        receipients = [{"address": nft["funding_recipient"]} for nft in self.scraper_data["NFTs"]]
        urls = self.s3.save_json_as_csv(receipients, self.bucket_name, f"ingestor_nfts_receipients_{self.asOf}")
        self.cyphers.queries.create_wallets(urls)

        urls = self.s3.save_json_as_csv(self.scraper_data["NFTs"], self.bucket_name, f"ingestor_nfts_{self.asOf}")
        self.cyphers.create_or_merge_NFTs(urls)
        self.cyphers.link_NFTs_to_articles(urls)
        self.cyphers.link_NFTs_to_owners(urls)
        self.cyphers.link_NFTs_to_receipient(urls)

    def run(self):
        self.ingest_articles()
        self.ingest_nfts()
        self.ingest_twitter()
        self.save_metadata()

if __name__ == '__main__':
    ingestor = MirrorIngestor()
    ingestor.run()