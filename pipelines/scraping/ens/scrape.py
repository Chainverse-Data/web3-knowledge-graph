from ..helpers import Scraper
from ..helpers import tqdm_joblib
import json
import logging
import tqdm
import os
import multiprocessing
import joblib
import math


class EnsScraper(Scraper):
    def __init__(self):
        super().__init__("ens")
        self.provider = "https://eth-mainnet.alchemyapi.io/v2/{}".format(os.environ["ALCHEMY_API_KEY"])
        self.headers = {"accept": "application/json"}

    def get_all_ens(self):
        logging.info("Getting all ENS...")
        self.data["ens"] = []
        with tqdm_joblib(tqdm.tqdm(desc="Getting ENS Data", total=len(self.data["owner_addresses"]))) as progress_bar:
            ens_list = joblib.Parallel(n_jobs=multiprocessing.cpu_count() - 1, backend="threading")(
                joblib.delayed(self.get_ens_info)(address) for address in self.data["owner_addresses"]
            )

        self.data["ens"] = [item for sublist in ens_list for item in sublist]
        self.data.pop("owner_addresses", None)
        logging.info("Found {} ENS".format(len(self.data["ens"])))

    def get_ens_info(self, address):
        token_list = []

        url = "https://eth-mainnet.g.alchemy.com/nft/v2/{}/getNFTs?owner={}&contractAddresses[]=0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85&withMetadata=true".format(
            os.environ["ALCHEMY_API_KEY"], address
        )
        try:
            content = self.get_request(url, headers=self.headers)
            data = json.loads(content)
            token_list.extend(data["ownedNfts"])
        except Exception as e:
            logging.error(f"Ens info error : {e}")
            data = {}

        pagekey = data.get("pageKey", None)
        while pagekey is not None:
            new_url = url + "&pageKey={}".format(pagekey)
            try:
                content = self.get_request(new_url, headers=self.headers)
                data = json.loads(content)
                token_list.extend(data["ownedNfts"])
            except Exception as e:
                logging.error(f"Ens info error : {e}")
                data = {}
            pagekey = data.get("pageKey", None)

        token_list = [
            {"name": entry["title"], "owner": address.lower(), "token_id": int(entry["id"]["tokenId"], base=16)}
            for entry in token_list
        ]

        return token_list

    def get_all_owner_addresses(self):
        logging.info("Getting all owner addresses...")
        self.data["owner_addresses"] = []
        url = "https://eth-mainnet.g.alchemy.com/nft/v2/{}/getOwnersForCollection?contractAddress=0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85&withTokenBalances=false".format(
            os.environ["ALCHEMY_API_KEY"]
        )
        content = self.get_request(url, headers=self.headers)
        data = json.loads(content)
        self.data["owner_addresses"] = data["ownerAddresses"]
        pageKey = data.get("pageKey", None)
        while pageKey is not None:
            new_url = url + "&pageKey={}".format(pageKey)
            content = self.get_request(new_url, headers=self.headers)
            data = json.loads(content)
            self.data["owner_addresses"] += data["ownerAddresses"]
            logging.info(f"{len(self.data['owner_addresses'])} current owners")
            pageKey = data.get("pageKey", None)

        logging.info("Found {} owner addresses".format(len(self.data["owner_addresses"])))

    def run(self):
        self.get_all_owner_addresses()
        self.get_all_ens()

        self.save_data()
        self.save_metadata()


if __name__ == "__main__":

    scraper = EnsScraper()
    scraper.run()
