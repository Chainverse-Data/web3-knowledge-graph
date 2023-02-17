from ..helpers import Scraper
import logging
import tqdm
import os
import web3
import multiprocessing
import joblib
import json
import warnings
DEBUG = os.environ.get("DEBUG", False)

class TwitterEnsScraper(Scraper):
    def __init__(self):
        super().__init__("twitter-ens")
        self.api_url = "https://ethleaderboard.xyz/api/frens?skip={}"
        # self.provider = "https://eth-mainnet.alchemyapi.io/v2/{}".format(os.environ["ALCHEMY_API_KEY"])
        # self.last_account_offset = 0
        # if "last_account_offset" in self.metadata:
        #     self.last_account_offset = self.metadata["last_account_offset"]

    def get_accounts(self):
        logging.info("Getting twitter ens accounts...")
        accounts = []
        offset = 0
        results = True
        if DEBUG:
            counter = 0
        while results:
            self.metadata["last_account_offset"] = offset
            if len(accounts) % 1000 == 0:
                logging.info(f"Current Accounts: {len(accounts)}")
            query = self.api_url.format(offset)
            data = json.loads(self.get_request(query))
            results = data["frens"]
            accounts.extend(results)
            offset += len(results)
            if DEBUG:
                counter += 1
                if counter > 10:
                    break
        logging.info(f"Total accounts aquired: {len(accounts)}")

        accounts = [{"ens": account["ens"], "handle": account["handle"]} for account in accounts]
        self.data["accounts"] = accounts

    def run(self):
        self.get_accounts()
        self.save_metadata()
        self.save_data()


if __name__ == "__main__":
    scraper = TwitterEnsScraper()
    scraper.run()
