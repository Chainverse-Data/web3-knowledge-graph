from ...helpers import Alchemy
from ..helpers import Scraper
import json
import logging
import tqdm
import os
import time
from datetime import datetime

DEBUG = os.environ.get("DEBUG", False)

class GitCoinScraper(Scraper):
    def __init__(self, bucket_name="gitcoin"):
        super().__init__(bucket_name)
        self.gitcoin_api_limit=40
        self.blocks_limit = 2000
        self.last_grant_offset = 0
        self.last_bounty_offset = 0
        self.last_block_number = 10245999 # block of the contract creation!
        if "last_grant_offset" in self.metadata:
            self.last_grant_offset = self.metadata["last_grant_offset"]
        if "last_bounty_offset" in self.metadata:
            self.last_bounty_offset = self.metadata["last_bounty_offset"]
        if "last_block_number" in self.metadata:
            self.last_block_number = self.metadata["last_block_number"]

        self.alchemy = Alchemy()
        self.gitcoin_checkout_contract_address = "0x7d655c57f71464B6f83811C55D84009Cd9f5221C"
        self.get_one_grant_url = "https://bounties.gitcoin.co/grants/v1/api/grant/{}/"
        self.get_all_grants_url = "https://bounties.gitcoin.co/api/v0.1/grants/?limit={}&offset={}"
        self.get_all_bounties_url = "https://bounties.gitcoin.co/api/v0.1/bounties/?limit={}&offset={}&order_by=web3_created"
    
    def get_all_grants(self):
        logging.info("Getting the list of all the grants from GitCoin")
        self.data["grants"] = []
        with tqdm.tqdm(total=self.gitcoin_api_limit, position=0) as pbar:
            while True:
                data = self.get_request(self.get_all_grants_url.format(self.gitcoin_api_limit, self.last_grant_offset), json=True)
                print(data)
                if len(data) == 0:
                    break
                for grant in tqdm.tqdm(data, position=1):
                    grant_data = self.get_request(self.get_one_grant_url.format(grant["id"]))
                    try:
                        grant_data = json.loads(grant_data)
                        self.data["grants"].append(grant_data["grants"])
                    except Exception as e:
                        logging.error("Some error occured parsing the JSON at \n {} \n {}".format(
                            self.get_one_grant_url.format(grant["id"]), grant_data))
                        raise(e)
                    time.sleep(1)
                self.last_grant_offset += self.gitcoin_api_limit
                self.metadata["last_grant_offset"] = self.last_grant_offset
                pbar.update(self.gitcoin_api_limit)
                pbar.total += self.gitcoin_api_limit
                pbar.refresh()
        logging.info("Success: Grants scrapped!")

    def get_all_donnations(self):
        logging.info("Collecting all events from GitCoin BulckCheckout and extracting DonationSent events")
        contract = self.get_smart_contract(self.gitcoin_checkout_contract_address)
        
        self.data["donations"] = []
        with tqdm.tqdm(total=self.blocks_limit) as pbar:
            while True:
                content = self.alchemy.getLogs(
                    self.gitcoin_checkout_contract_address, 
                    fromBlock=self.last_block_number, 
                    toBlock=self.last_block_number + self.blocks_limit
                )
                tx_done = []
                if content == None:
                    break
                if DEBUG and len(self.data["donations"]) > 100:
                    break
                for event in content:
                    if event["transactionHash"] not in tx_done:
                        tx_logs = self.parse_logs(contract, event["transactionHash"], "DonationSent")
                        for log in tx_logs:
                            tmp = dict(log['args'])
                            tmp["txHash"] = event["transactionHash"]
                            tmp["chain"] = "Ethereum"
                            tmp["chainId"] = "1"
                            tmp["blockNumber"] = int(event["blockNumber"], base=16)
                            self.data["donations"].append(tmp)
                        tx_done.append(event["transactionHash"])
                self.metadata["last_block_number"] = self.last_block_number
                self.last_block_number += self.blocks_limit
                pbar.update(self.blocks_limit)
                pbar.total += self.blocks_limit
                pbar.refresh()
        logging.info("Success: Donations scrapped!")

    def get_all_bounties(self):
        logging.info("Getting the list of all the bounties from GitCoin")
        self.data["bounties"] = []
        with tqdm.tqdm(total=self.gitcoin_api_limit) as pbar:
            while True:
                content = self.get_request(self.get_all_bounties_url.format(self.gitcoin_api_limit, self.last_bounty_offset))
                data = json.loads(content)
                if len(data) == 0:
                    break
                self.data["bounties"] += data
                self.last_bounty_offset += self.gitcoin_api_limit
                self.metadata["last_bounty_offset"] = self.last_bounty_offset
                pbar.update(self.gitcoin_api_limit)
                pbar.total += self.gitcoin_api_limit
                pbar.refresh()
        logging.info("Success: Bounties scrapped!")

    def run(self):
        self.get_all_donnations()
        self.get_all_grants()
        self.get_all_bounties()
        self.save_data()
        self.save_metadata()

if __name__ == "__main__":
    scraper = GitCoinScraper()
    scraper.run()