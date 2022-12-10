import time
from ..helpers import Scraper
from ..helpers import str2bool
import os
import argparse
import gql 
import logging 
from dotenv import load_dotenv
from gql.transport.aiohttp import AIOHTTPTransport, log as gql_log

gql_log.setLevel(logging.WARNING)

class DelegationScraper(Scraper):
    
    def __init__(self, bucket_name='gitcoin-delegation-test', allow_override=True):
        super().__init__(bucket_name, allow_override=allow_override)
        self.gitcoin_url =   f'https://gateway.thegraph.com/api/{graph_api_key}/subgraphs/id/QmQXuWMr5zgDWwHtc4MzzJkkWZinTZQjGEQe5ES9BtQ5Bf'
        self.metadata['cutoff_block'] = self.metadata.get("cutoff_block", 13018595)
        self.data['delegations'] = ['init']
        self.interval = 1000
