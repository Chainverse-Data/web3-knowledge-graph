from ..helpers import Scraper
from ..helpers import tqdm_joblib
import logging
import tqdm
import os
import web3
import multiprocessing
import joblib
import json
import warnings


class TwitterEnsScraper(Scraper):
    def __init__(self):
        super().__init__("twitter-ens")
        self.api_url = "https://ethleaderboard.xyz/api/frens?skip={}"
        self.provider = "https://eth-mainnet.alchemyapi.io/v2/{}".format(os.environ["ALCHEMY_API_KEY"])
        self.last_account_offset = 0
        if "last_account_offset" in self.metadata:
            self.last_account_offset = self.metadata["last_account_offset"]

    def get_accounts(self):
        logging.info("Getting twitter ens accounts...")
        accounts = []
        offset = self.last_account_offset
        results = True
        while results:
            self.metadata["last_account_offset"] = offset
            if len(accounts) % 1000 == 0:
                logging.info(f"Current Accounts: {len(accounts)}")
            query = self.api_url.format(offset)
            data = json.loads(self.get_request(query))
            results = data["frens"]
            accounts.extend(results)
            offset += len(results)
        logging.info(f"Total accounts aquired: {len(accounts)}")

        accounts = [{"ens": account["ens"], "handle": account["handle"]} for account in accounts]
        self.data["accounts"] = accounts

        # with tqdm_joblib(tqdm.tqdm(desc="Getting ENS Data", total=len(accounts))) as progress_bar:
        #     address_list = joblib.Parallel(n_jobs=multiprocessing.cpu_count() - 1, backend="threading")(
        #         joblib.delayed(self.get_ens_address)(account["ens"]) for account in accounts
        #     )
        # final_accounts = []
        # for i in range(len(accounts)):
        #     if address_list[i] is not None:
        #         accounts[i]["address"] = address_list[i]
        #         final_accounts.append(accounts[i])

        # final_accounts = [account for account in final_accounts if account]
        # self.data["accounts"] = final_accounts
        # logging.info(f"Final accounts count: {len(final_accounts)}")

    def get_ens_address(self, name):
        warnings.filterwarnings("ignore")
        w3 = web3.Web3(web3.Web3.HTTPProvider(self.provider))
        try:
            owner = w3.ens.address(name=name)
            if owner is not None:
                owner = owner.lower()
        except:
            owner = None
        return owner

    def run(self):
        self.get_accounts()

        self.save_metadata()
        self.save_data()


if __name__ == "__main__":

    scraper = TwitterEnsScraper()
    scraper.run()
