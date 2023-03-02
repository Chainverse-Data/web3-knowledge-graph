import logging
from tqdm import tqdm
from ..helpers import Processor
from .cyphers import LastActivityCyphers
from datetime import datetime
import os
import json
import time

DEBUG = os.environ.get("DEBUG", False)


class LastActivityPostProcess(Processor):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""

    def __init__(self):
        self.cyphers = LastActivityCyphers()
        super().__init__("last-activity")
        self.alchemy_endpoints = {
            "ethereum": "https://eth-mainnet.g.alchemy.com/v2/{}".format(os.environ["ALCHEMY_API_KEY"]),
            # "optimism": "https://opt-mainnet.g.alchemy.com/v2/{}".format(os.environ["ALCHEMY_API_KEY_OPTIMISM"]),
            # "arbitrum": "https://arb-mainnet.g.alchemy.com/v2/{}".format(os.environ["ALCHEMY_API_KEY_ARBITRUM"]),
            # "polygon": "https://polygon-mainnet.g.alchemy.com/v2/{}".format(os.environ["ALCHEMY_API_KEY_POLYGON"])
        }
        print(self.alchemy_endpoints)
        self.categories = {
            "ethereum": ["external","internal","erc20","erc721","erc1155","specialnft"],
            "optimism": ["external","erc20","erc721","erc1155","specialnft"],
            "arbitrum": ["external","erc20","erc721","erc1155","specialnft"],
            "polygon": ["external","internal","erc20","erc721","erc1155","specialnft"]
        }

    def alchemy_API_call(self, payload, chain, key, counter=0):
        if counter > 10:
            time.sleep(counter)
            return []
        headers = {"Content-Type": "application/json"}
        alchemy_api_url = self.alchemy_endpoints[chain]
        print(alchemy_api_url, payload, headers)
        content = self.post_request(alchemy_api_url, json=payload, headers=headers)
        content = json.loads(content)
        print(content)
        result = content.get("result", None)
        if not result:
            return self.alchemy_API_call(payload, chain, key, counter=counter+1)
        return result[key]

    def get_block_timestamp(self, block, chain):
        payload = f"""
            {{
                "jsonrpc": "2.0",
                "id": 0,
                "method": "eth_getBlockByNumber",
                "params": [
                    "{block}",
                    false
                ]
            }}"""
        timestamp = self.alchemy_API_call(payload, chain, "timestamp")
        timestamp = int(timestamp, 16)
        timestamp = datetime.fromtimestamp(timestamp)
        return timestamp

    def get_wallet_tx(self, wallet, chain, sort="asc"):
        categories = ['"' + category + '"' for category in self.categories[chain]]
        categories = f'[{",".join(categories)}]'
        payload = f"""
        {{
            "jsonrpc": "2.0",
            "id": 0,
            "method": "alchemy_getAssetTransfers",
            "params": [
                {{
                "toBlock": "latest",
                "fromAddress": "{wallet}",
                "maxCount": "0x1",
                "excludeZeroValue": false,
                "order": "{sort}",
                "category": {categories}
                }}
            ]
        }}"""
        return self.alchemy_API_call(payload, chain, "transfers")

    def get_wallets(self):
        self.wallets = self.cyphers.get_all_wallets()

    def get_tx(self, wallet, sort):
        results = {"address": wallet}
        for chain in self.alchemy_endpoints:
            transactions = self.get_wallet_tx(wallet, chain, sort=sort)
            print(transactions)
            if len(transactions) > 0:
                block = transactions[0]["block"]
                timestamp = self.get_block_timestamp(block, chain)
                results[chain] = timestamp
            else:
                results[chain] = None
        return results

    def get_last_tx(self, wallet):
        return self.get_tx(wallet, "asc")

    def get_fisrt_tx(self, wallet):
        return self.get_tx(wallet, "desc")

    def process_last_transactions(self):
        logging.info("Processing last transaction for all wallets")
        wallets = self.cyphers.get_all_wallets_without_first_tx()
        data = self.parallel_process(self.get_last_tx, wallets, description="Getting last transactions data")
        urls = self.save_json_as_csv(data, self.bucket_name, f"processor_last_transactions-{self.asOf}")
        self.cyphers.set_last_active_date(urls)
        logging.info("Last transactions done")

    def process_first_transactions(self):
        logging.info("Processing first transaction for all wallets")
        wallets = self.cyphers.get_all_wallets_without_first_tx()
        data = self.parallel_process(self.get_fisrt_tx, wallets, description="Getting first transactions data")
        urls = self.save_json_as_csv(data, self.bucket_name, f"processor_first_transactions-{self.asOf}")
        self.cyphers.set_first_active_date(urls)
        logging.info("first transactions done")

    def run(self):
        self.process_first_transactions()
        self.process_last_transactions()

if __name__ == "__main__":
    processor = LastActivityPostProcess()
    processor.run()
