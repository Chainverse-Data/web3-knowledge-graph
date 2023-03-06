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
            "optimism": "https://opt-mainnet.g.alchemy.com/v2/{}".format(os.environ["ALCHEMY_API_KEY_OPTIMISM"]),
            "arbitrum": "https://arb-mainnet.g.alchemy.com/v2/{}".format(os.environ["ALCHEMY_API_KEY_ARBITRUM"]),
            "polygon": "https://polygon-mainnet.g.alchemy.com/v2/{}".format(os.environ["ALCHEMY_API_KEY_POLYGON"])
        }
        self.categories = {
            "ethereum": ["external","internal","erc20","erc721","erc1155","specialnft"],
            "optimism": ["external","erc20","erc721","erc1155","specialnft"],
            "arbitrum": ["external","erc20","erc721","erc1155","specialnft"],
            "polygon": ["external","internal","erc20","erc721","erc1155","specialnft"]
        }
        self.chunk_size = 100000

    def alchemy_API_call(self, payload, chain, key, counter=0):
        if counter > 3:
            time.sleep(counter)
            return []
        headers = {"Content-Type": "application/json"}
        alchemy_api_url = self.alchemy_endpoints[chain]
        content = self.post_request(alchemy_api_url, data=payload, headers=headers, return_json=True)
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

    def get_tx(self, wallet, sort):
        results = {"address": wallet["address"]}
        for chain in self.alchemy_endpoints:
            if chain in wallet and wallet[chain]:
                results[chain] = wallet[chain].timestamp()
            else:
                transactions = self.get_wallet_tx(wallet["address"], chain, sort=sort)
                if len(transactions) > 0:
                    block = transactions[0]["blockNum"]
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
        wallets = self.cyphers.get_all_wallets()
        for i in tqdm(range(0, len(wallets), self.chunk_size), position=0, desc="Wallet chunks"):
            data = self.parallel_process(self.get_last_tx, wallets[i: i+self.chunk_size], description="Getting last transactions data")
            for chain in self.alchemy_endpoints:
                tmp = [{"address": element["address"], "date": element[chain]} for element in data if element[chain]]
                urls = self.save_json_as_csv(tmp, self.bucket_name, f"processor_last_transactions_{chain}-{self.asOf}_{i}")
                self.cyphers.set_last_active_date(urls, chain)
        logging.info("Last transactions done")

    def process_first_transactions(self):
        logging.info("Processing first transaction for all wallets")
        wallets = self.cyphers.get_all_wallets_without_first_tx()
        for i in tqdm(range(0, len(wallets), self.chunk_size), position=0, desc="Wallet chunks"):
            data = self.parallel_process(self.get_fisrt_tx, wallets[i: i+self.chunk_size], description="Getting first transactions data")
            for chain in self.alchemy_endpoints:
                tmp = [{"address": element["address"], "date": element[chain]} for element in data if element[chain]]
                urls = self.save_json_as_csv(tmp, self.bucket_name, f"processor_first_transactions_{chain}-{self.asOf}_{i}")
                self.cyphers.set_first_active_date(urls, chain)
        logging.info("first transactions done")

    def run(self):
        self.process_first_transactions()
        self.process_last_transactions()

if __name__ == "__main__":
    processor = LastActivityPostProcess()
    processor.run()
