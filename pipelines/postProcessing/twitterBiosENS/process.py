import logging
import pandas as pd
from tqdm import tqdm
from ..helpers import Processor
from .cyphers import TwitterENSBiosCyphers
from ...helpers import S3Utils
from urllib.parse import urlparse, urlunparse
from datetime import datetime, timedelta
import os
import re
import requests as r
import datetime
import time

DEBUG = os.environ.get("DEBUG", False)

class TwitterBiosENSProcessor(Processor):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""

    def __init__(self):
        self.cyphers = TwitterENSBiosCyphers()
        self.ens_matcher = re.compile("([-a-zA-Z0-9@:%._\+~#=]{1,256}\.eth)")
        super().__init__("twitter-ens-bios3")

    def extract_ens(self, bio):
        matches = self.ens_matcher.findall(bio)
        if not matches:
            return None 
        ens = [match.strip() for match in matches]
        return ens

    def extract_accounts_from_bio(self) -> pd.DataFrame:
        accounts = self.cyphers.get_bios()
        accounts = [{"handle": str(account["handle"]), "bio": str(account["bio"])} for account in accounts]
        accounts_df = pd.DataFrame.from_dict(accounts)
        accounts_df['ens'] = accounts_df['bio'].apply(self.extract_ens)
        accounts_df = accounts_df.dropna(subset=['ens'])
        accounts_df = accounts_df.explode('ens')
        logging.info(f"Twitter with ENS in bio: {len(accounts_df)}")
        return accounts_df 

    def process_ens_in_bio(self):
        accounts = self.extract_accounts_from_bio()
        urls = self.save_df_as_csv(accounts, f"processing_bios_ens_{self.asOf}")
        self.cyphers.link_twitter_ens(urls)

    def run(self):
        self.process_ens_in_bio()

if __name__ == "__main__":
    processor = TwitterBiosENSProcessor()
    processor.run()
