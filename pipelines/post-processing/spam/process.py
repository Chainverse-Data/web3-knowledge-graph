import logging
from tqdm import tqdm
from ..helpers import Processor
from .cyphers import SpamCyphers
import os
import requests



class SpamProcessor(Processor):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""
    def __init__(self):
        self.cyphers = SpamCyphers()
        super().__init__("spam")
        
        self.alchemy_mainnet_url = f"https://eth-mainnet.g.alchemy.com/nft/v2/{os.environ['ALCHEMY_API_KEY']}/getSpamContracts"
        self.headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }

    def get_mainnet_spam_contracts(self):
        ## it's a list
        response_data = self.get_request(self.alchemy_mainnet_url, headers=self.headers, json=True)        
        if response_data:
            spamAddresses = [address.lower() for address in response_data]
            self.cyphers.label_spam_contracts(spamAddresses)            

    def run(self):
        self.get_mainnet_spam_contracts()

if __name__ == '__main__':
    processor = SpamProcessor()
    processor.run()
