import logging
from tqdm import tqdm
from ..helpers import Processor
# from .cyphers import SpamCyphers
from datetime import datetime
import os
import json
import requests
import time

DEBUG = os.environ.get("DEBUG", False)


class SpamProcessor(Processor):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""
    def __init__(self):
        #self.cyphers = SpamCyphers()
        super().__init__("spam")
        self.alchemy_mainnet_url = "https://eth-mainnet.g.alchemy.com/v2/{}/getSpamContracts".format(os.environ["ALCHEMY_API_KEY"]),
        self.alchemy_polygon_url =  "https://polygon-mainnet.g.alchemy.com/v2/{}/getSpamContracts".format(os.environ["ALCHEMY_API_KEY_POLYGON"])
        self.headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }

    def get_mainnet_spam_contracts(self):
        ## it's a list
        url = self.alchemy_mainnet_url
        response_data = requests.get(url, headers=self.headers)
        if type(response_data) != dict:
            result = {}
        else:
            result = response_data
            logging.info(result)
            results = len(results)
        return None
    def run(self):
        self.get_mainnet_spam_contracts()

if __name__ == '__main__':
    processor = SpamProcessor()
    processor.run()
