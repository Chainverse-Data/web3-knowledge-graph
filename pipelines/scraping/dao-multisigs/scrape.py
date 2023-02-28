from ..helpers import Scraper
import logging
import requests as r
import time
import os
import pandas as pd
import logging
import Cypher

DEBUG = os.environ.get("DEBUG", False)

class MultisigTxScraper(Scraper):
    def __init__(self, bucket_name="multisig-transactions", allow_override=False):
        super().__init__(bucket_name, allow_override=allow_override)
        super().__init__(database)
        self.alchemy_api_url = "https://eth-mainnet.g.alchemy.com/v2/{}".format(os.environ["ALCHEMY_API_KEY"])

    def get_multisig_wallets(self):
        query = """
        match
            (e:Entity)-[r:HAS_WALLET|HAS_SAFE]->(w:Wallet)
        return distinct
            w.address as address
        """

        ## as a df 
        return dao_wallet_df 

    def get_txes(self, wallet_address=None):
        request_payload = {
            "id": 1, 
            "jsonrpc": "2.0",
            "method": "alchemy_getAssetTransfers",
            "params": [
                "fromBlock": "0x0",
                "toBlock": "latest",

            ]


        }

      payload = {
    "id": 1,
    "jsonrpc": "2.0",
    "method": "alchemy_getAssetTransfers",
    "params": [
        {
            "fromBlock": "0x0",
            "toBlock": "latest",
            "toAddress": "0x5c43B1eD97e52d009611D89b74fA829FE4ac56b1",
            "category": ["external"],
            "withMetadata": False,
            "excludeZeroValue": True,
            "maxCount": "0x3e8"
        }
    ]
}
headers = {
    "accept": "application/json",
    "content-type": "application/json"
}
  