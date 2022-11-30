import time
import json
import os
from tqdm import tqdm
from ..helpers import Scraper
from .cyphers import TokenHoldersCypher
import logging

# I have to add try except statements to this because the overall api calls from alchemy have a 1/20000 chance of SSL failure.
# This is an issue as there are over a million wallets as of dec 2022
# For the future, there should be a better way to do this.

class TokenHolderScraper(Scraper):
    def __init__(self, bucket_name="token-holders"):
        super().__init__(bucket_name)
        self.cyphers = TokenHoldersCypher()
        self.wallets_last_block = self.metadata.get("wallets_last_block", {})
        self.alchemy_api_url = "https://eth-mainnet.g.alchemy.com/v2/{}".format(os.environ["ALCHEMY_API_KEY"])
        self.get_current_block()

    def get_current_block(self):
        headers = {"Content-Type": "application/json"}
        payload = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "eth_blockNumber"
        }
        content = self.post_request(self.alchemy_api_url, json=payload, headers=headers)
        content = json.loads(content)
        self.current_block = int(content["result"], 16)

    def get_all_wallets_in_db(self):
        self.wallet_list  = self.cyphers.get_all_wallets()

    def get_transactions_assets_balances(self):
        logging.info("Getting all transactions assets and balances")
        transactions = {}
        balances = {}
        assets = {}
        tokens = {}
        for wallet in tqdm(self.wallet_list):
            assets[wallet] = set()
            transactions[wallet] = {}
            transactions[wallet]["received"] = self.get_received_transactions(wallet, self.wallets_last_block.get(wallet, 0))
            transactions[wallet]["sent"] = self.get_sent_transactions(wallet, self.wallets_last_block.get(wallet, 0))
            for transaction in transactions[wallet]["received"] + transactions[wallet]["sent"]:
                if transaction["category"] in ["erc20", "erc721", "erc1155"]:
                    contractAddress = transaction["rawContract"]["address"]
                    if contractAddress not in tokens:
                        tokens[contractAddress] = {
                            "contractType": transaction["category"],
                            "symbol": transaction["asset"],
                            "decimal": transaction["rawContract"]["decimal"],
                        }
                    assets[wallet].add(contractAddress)
            assets[wallet] = list(assets[wallet])
            balances[wallet] = self.get_balances(wallet, assets[wallet])
            self.wallets_last_block[wallet] = self.current_block
        self.data["transactions"] = transactions
        self.data["balances"] = balances
        self.data["assets"] = assets
        self.data["tokens"] = tokens

    def alchemy_API_call_iterate(self, payload, key, pagekey=1, counter=0, results=[]):
        if counter > 10:
            time.sleep(counter)
            return results
        headers = {"Content-Type": "application/json"}
        while pagekey:
            content = self.post_request(self.alchemy_api_url, json=payload, headers=headers)
            content = json.loads(content)
            result = content.get("result", None)
            if not result:
                return self.alchemy_API_call_iterate(payload, key, pagekey=pagekey, counter=counter+1, results=results)
            pagekey = result.get("pagekey", None)
            if pagekey:
                payload["params"][0]["pagekey"] = pagekey
            results += result[key]
        return results

    def get_sent_transactions(self, address, start_block):
        payload = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "alchemy_getAssetTransfers",
            "params": [
                {
                    "fromBlock": hex(start_block),
                    "toBlock": hex(self.current_block),
                    "category": ["erc20", "erc721", "erc1155"],
                    "withMetadata": False,
                    "excludeZeroValue": True,
                    "maxCount": "0x3e8",
                    "order": "desc",
                    "fromAddress": str(address),
                }
            ],
        }
        try:
            transactions = self.alchemy_API_call_iterate(
                payload, "transfers", pagekey=1, counter=0, results=[])
        except:
            logging.error("There has been an error getting information about the address: ", address)
        return transactions

    def get_received_transactions(self, address, start_block):
        payload = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "alchemy_getAssetTransfers",
            "params": [
                {
                    "fromBlock": hex(start_block),
                    "toBlock": hex(self.current_block),
                    "category": ["erc20", "erc721", "erc1155"],
                    "withMetadata": False,
                    "excludeZeroValue": True,
                    "maxCount": "0x3e8",
                    "order": "desc",
                    "toAddress": str(address),
                }
            ],
        }
        try:
            transactions = self.alchemy_API_call_iterate(
                payload, "transfers", pagekey=1, counter=0, results=[])
        except:
            logging.error("There has been an error getting information about the address: ", address)
        return transactions

    def get_balances(self, wallet, tokenList):
        if len(tokenList) == 0:
            return []
        payload = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "alchemy_getTokenBalances",
            "params": [
                wallet, 
                tokenList
            ]
        }
        try:
            token_balances = self.alchemy_API_call_iterate(payload, "tokenBalances", pagekey=1, counter=0, results=[])
        except:
            logging.error("There has been an error getting information about the address: ", wallet)
        return token_balances

    def run(self):
        self.get_all_wallets_in_db()
        self.get_transactions_assets_balances()
        self.save_data()
        self.save_metadata()

if __name__ == "__main__":
    scraper = TokenHolderScraper()
    scraper.run()