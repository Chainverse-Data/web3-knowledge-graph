import logging
from tqdm import tqdm
from ...helpers import Alchemy
from ..helpers import Processor
from .cyphers import LastActivityCyphers
import os
import time

DEBUG = os.environ.get("DEBUG", False)


class LastActivityPostProcess(Processor):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""

    def __init__(self):
        self.cyphers = LastActivityCyphers()
        super().__init__("last-activity")
        self.alchemy = Alchemy()
        self.chunk_size = 10000

    def get_tx(self, wallet, sort):
        results = {"address": wallet["address"]}
        for chain in self.alchemy.chains:
            if chain in wallet and wallet[chain]:
                results[chain] = wallet[chain].timestamp()
            else:
                transactions = self.alchemy.getAssetTransfers(
                    toBlock="latest", 
                    fromAddress=wallet["address"], 
                    maxCount=1, 
                    chain=chain,
                    order=sort,
                    excludeZeroValue=False,
                    pageKeyIterate=False
                    )
                if len(transactions) > 0:
                    block = transactions[0]["blockNum"]
                    timestamp = self.alchemy.getBlockByNumber(block, chain=chain)
                    timestamp = int(timestamp["timestamp"], 16)
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
            for chain in self.alchemy.chains:
                tmp = [{"address": element["address"], "date": element[chain]} for element in data if element[chain]]
                urls = self.save_json_as_csv(tmp, self.bucket_name, f"processor_last_transactions_{chain}-{self.asOf}_{i}")
                self.cyphers.set_last_active_date(urls, chain)
        logging.info("Last transactions done")

    def process_first_transactions(self):
        logging.info("Processing first transaction for all wallets")
        wallets = self.cyphers.get_all_wallets_without_first_tx()
        for i in tqdm(range(0, len(wallets), self.chunk_size), position=0, desc="Wallet chunks"):
            data = self.parallel_process(self.get_fisrt_tx, wallets[i: i+self.chunk_size], description="Getting first transactions data")
            for chain in self.alchemy.chains:
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
