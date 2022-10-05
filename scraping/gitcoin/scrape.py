from ..helpers import Scraper
from ..helpers import get_smart_contract, parse_logs
import json
import logging
import tqdm
import os

class GitCoinScraper(Scraper):
    def __init__(self):
        super().__init__("gitcoin", allow_override=False)
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

        self.gitcoin_checkout_contract_address = "0x7d655c57f71464B6f83811C55D84009Cd9f5221C"
        self.get_one_grant_url = "https://gitcoin.co/grants/v1/api/grant/{}/"
        self.get_all_grants_url = "https://gitcoin.co/api/v0.1/grants/?limit={}&offset={}"
        self.get_donations_url = "https://eth-mainnet.g.alchemy.com/v2/{}".format(os.environ["ALCHEMY_API_KEY"])
        self.get_all_bounties_url = "https://gitcoin.co/api/v0.1/bounties/?limit={}&offset={}&order_by=web3_created"
    
    def get_all_grants(self):
        logging.info("Getting the list of all the grants from GitCoin")
        self.data["grants"] = []
        with tqdm.tqdm(total=self.gitcoin_api_limit, position=0) as pbar:
            while True:
                content = self.get_request(self.get_all_grants_url.format(self.gitcoin_api_limit, self.last_grant_offset))
                data = json.loads(content)
                if len(data) == 0:
                    break
                for grant in tqdm.tqdm(data, position= 1):
                    grant_data = self.get_request(self.get_one_grant_url.format(grant["id"]))
                    try:
                        grant_data = json.loads(grant_data)
                    except Exception as e:
                        logging.error("Some error occured parsing the JSON: ", grant_data)
                        raise(e)
                    self.data["grants"].append(grant_data["grants"])
                self.last_grant_offset += self.gitcoin_api_limit
                self.metadata["last_grant_offset"] = self.last_grant_offset
                pbar.update(self.gitcoin_api_limit)
                pbar.total += self.gitcoin_api_limit
                pbar.refresh()

    def get_all_donnations(self):
        logging.info("Collecting all events from GitCoin BulckCheckout and extracting DonationSent events")
        contract = get_smart_contract(self.gitcoin_checkout_contract_address)
        headers = {"Content-Type": "application/json"}
        post_data = {
            "jsonrpc":"2.0", 
            "id": 0,
            "method":"eth_getLogs",
            "params": [
                         {
                            "fromBlock": hex(self.last_block_number),
                            "toBlock": hex(self.last_block_number + self.blocks_limit),
                            "address": self.gitcoin_checkout_contract_address
                          }
                      ]
        }
        self.data["donations"] = []
        with tqdm.tqdm(total=self.blocks_limit) as pbar:
            while True:
                content = self.post_request(self.get_donations_url, json=post_data, headers=headers)
                content = json.loads(content)
                tx_done = []
                if "result" not in content:
                    break
                for event in content["result"]:
                    if event["transactionHash"] not in tx_done:
                        tx_logs = parse_logs(contract, event["transactionHash"], "DonationSent")
                        for log in tx_logs:
                            tmp = dict(log['args'])
                            tmp["txHash"] = event["transactionHash"]
                            tmp["chain"] = "Ethereum"
                            tmp["chainId"] = "1"
                            self.data["donations"].append(tmp)
                        tx_done.append(event["transactionHash"])
                self.metadata["last_block_number"] = self.last_block_number
                self.last_block_number += self.blocks_limit
                post_data["params"][0]["fromBlock"] = hex(self.last_block_number)
                post_data["params"][0]["toBlock"] = hex(self.last_block_number + self.blocks_limit)
                pbar.update(self.blocks_limit)
                pbar.total += self.blocks_limit
                pbar.refresh()

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

    def run(self):
        self.get_all_donnations()
        self.get_all_grants()
        self.get_all_bounties()
        self.save_data()
        self.save_metadata()

if __name__ == "__main__":
    scraper = GitCoinScraper()
    scraper.run()