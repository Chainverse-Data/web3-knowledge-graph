import multiprocessing
import time
import json
import os
import joblib
from tqdm import tqdm
from ..helpers import Scraper
from .cyphers import TokenHoldersCypher
import logging
from ..helpers import tqdm_joblib

DEBUG = os.environ.get("DEBUG", False)
# I have to add try except statements to this because the overall api calls from alchemy have a 1/20000 chance of SSL failure.
# This is an issue as there are over a million wallets as of dec 2022
# For the future, there should be a better way to do this.

class TokenHolderScraper(Scraper):
    def __init__(self, bucket_name="token-holders", allow_override=False):
        super().__init__(bucket_name, allow_override=allow_override)
        self.cyphers = TokenHoldersCypher()
        self.wallets_last_block = self.metadata.get("wallets_last_block", {})
        self.alchemy_api_url = "https://eth-mainnet.g.alchemy.com/v2/{}".format(os.environ["ALCHEMY_API_KEY"])
        self.get_current_block()
        self.max_thread = multiprocessing.cpu_count() * 2
        self.important_only = os.environ.get("IMPORTANT_WALLETS", False)
        if DEBUG:
            self.max_thread = multiprocessing.cpu_count() - 1
        os.environ["NUMEXPR_MAX_THREADS"] = str(self.max_thread)

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
        if self.important_only:
            self.wallet_list  = self.cyphers.get_important_wallets()
        else:
            self.wallet_list  = self.cyphers.get_all_wallets()

    def job_get_transactions(self, wallet):
        assets = set()
        transactions = {}
        tokens = {}
        transactions["received"] = self.get_received_transactions(wallet, self.wallets_last_block.get(wallet, 0))
        transactions["sent"] = self.get_sent_transactions(wallet, self.wallets_last_block.get(wallet, 0))
        for transaction in transactions["received"] + transactions["sent"]:
            if transaction["category"] in ["erc20", "erc721", "erc1155"]:
                contractAddress = transaction["rawContract"]["address"]
                if contractAddress not in tokens:
                    tokens[contractAddress] = {
                        "contractType": transaction["category"],
                        "symbol": transaction["asset"],
                        "decimal": transaction["rawContract"]["decimal"],
                    }
                assets.add(contractAddress)
        assets = list(assets)
        return (wallet, assets, tokens)

    def job_get_balances(self, wallet):
        balances = self.get_balances(wallet, self.data["assets"][wallet])
        return (wallet, balances)

    def get_transactions_assets_balances(self, wallets):
        logging.info("Getting all transactions assets and balances")
        self.data["balances"] = {}
        self.data["assets"] = {}
        self.data["tokens"] = {}
        logging.info("Multithreaded scraping launching!")
        with tqdm_joblib(tqdm(desc="Getting transactions data", total=len(wallets))):
            data = joblib.Parallel(n_jobs=self.max_thread, backend="threading")(joblib.delayed(self.job_get_transactions)(wallet) for wallet in wallets)
        for item in tqdm(data):
            wallet, assets, tokens = item
            self.data["assets"][wallet] = assets
            for token in tokens:
                if token not in self.data["tokens"]:
                    self.data["tokens"][token] = tokens[token]
        
        with tqdm_joblib(tqdm(desc="Getting balances data", total=len(wallets))):
            data = joblib.Parallel(n_jobs=self.max_thread, backend="threading")(joblib.delayed(self.job_get_balances)(wallet) for wallet in wallets)
        for item in tqdm(data):
            wallet, balances = item
            self.data["balances"][wallet] = balances

        for wallet in tqdm(wallets):
            self.wallets_last_block[wallet] = self.current_block

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
            return transactions
        except:
            logging.error(f"There has been an error getting information about the address: {address}")
            return []

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
            return transactions
        except:
            logging.error(f"There has been an error getting information about the address: {address}")
            return []

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
            return token_balances
        except:
            logging.error(f"There has been an error getting information about the address: {wallet}")
            return []

    def run(self):
        self.get_all_wallets_in_db()
        chunk_id = 0
        chunk_size = 100000
        for i in tqdm(range(0, len(self.wallet_list), chunk_size)):
            logging.info(f"Now scraping wallet chunk: {chunk_id}")
            self.get_transactions_assets_balances(self.wallet_list[i:i+chunk_size])
            self.save_data(chunk_prefix=chunk_id)
            self.data = {}
            self.metadata["wallets_last_block"] = self.wallets_last_block
            self.save_metadata()
            chunk_id += 1


if __name__ == "__main__":
    scraper = TokenHolderScraper()
    scraper.run()