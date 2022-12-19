from ..helpers import Scraper
from ..helpers import tqdm_joblib
import json
import logging
import tqdm
import os
import multiprocessing
import joblib
import warnings
import web3
from ens.auto import ns


class EnsScraper(Scraper):
    def __init__(self, bucket_name="ens", allow_override=False):
        super().__init__(bucket_name, allow_override=allow_override)
        self.provider = "https://eth-mainnet.alchemyapi.io/v2/{}".format(os.environ["ALCHEMY_API_KEY"])
        self.headers = {"accept": "application/json"}
        self.max_threads = multiprocessing.cpu_count() * 2
        os.environ["NUMEXPR_MAX_THREADS"] = str(self.max_threads)

    def get_all_ens(self):
        logging.info("Getting all ENS...")
        self.data["ens"] = []
        with tqdm_joblib(tqdm.tqdm(desc="Getting ENS Data", total=len(self.data["owner_addresses"]))):
            ens_list = joblib.Parallel(n_jobs=self.max_threads, backend="threading")(
                joblib.delayed(self.get_ens_info)(address) for address in self.data["owner_addresses"]
            )

        self.data["ens"] = [item for sublist in ens_list for item in sublist]

        with tqdm_joblib(tqdm.tqdm(desc="Getting Primary Names", total=len(self.data["owner_addresses"]))):
            primary_list = joblib.Parallel(n_jobs=self.max_threads, backend="threading")(
                joblib.delayed(self.get_primary_info)(address) for address in self.data["owner_addresses"]
            )
        primary_list = [item for item in primary_list if item is not None]
        self.data["primary"] = primary_list

        self.data.pop("owner_addresses", None)
        logging.info("Found {} ENS".format(len(self.data["ens"])))

    def get_primary_info(self, address):
        warnings.filterwarnings("ignore")
        x = ns.fromWeb3(web3.Web3(web3.Web3.HTTPProvider(self.provider)))
        name = x.name(web3.Web3.toChecksumAddress(address))
        if name is not None:
            return {"name": name, "address": address.lower()}
        return None

    def get_ens_info(self, address):
        token_list = []
        url = "https://eth-mainnet.g.alchemy.com/nft/v2/{}/getNFTs?owner={}&contractAddresses[]=0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85&withMetadata=true".format(
            os.environ["ALCHEMY_API_KEY"], address
        )
        new_url = url
        page_key = 1
        while page_key is not None:
            content = self.get_request(new_url, headers=self.headers)
            if content is None:
                break
            data = json.loads(content)
            token_list.extend(data["ownedNfts"])
            page_key = data.get("pageKey", None)
            new_url = url + "&pageKey={}".format(page_key)

        token_list = [
            {"name": entry["title"], "address": address.lower(), "token_id": int(entry["id"]["tokenId"], base=16)}
            for entry in token_list
        ]

        return token_list

    def get_all_owner_addresses(self):
        logging.info("Getting all owner addresses...")
        self.data["owner_addresses"] = []
        url = "https://eth-mainnet.g.alchemy.com/nft/v2/{}/getOwnersForCollection?contractAddress=0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85&withTokenBalances=false".format(os.environ["ALCHEMY_API_KEY"])
        page_key = 0
        new_url = url
        while page_key is not None:
            content = self.get_request(new_url, headers=self.headers)
            if content is None:
                break
            data = json.loads(content)
            self.data["owner_addresses"] += data["ownerAddresses"]
            logging.info(f"{len(self.data['owner_addresses'])} current owners")
            page_key = data.get("pageKey", None)
            new_url = url + "&pageKey={}".format(page_key)
        if len(self.data["owner_addresses"]) == 0:
            raise Exception("Something went wrong getting the ENS Owners ...")
        self.data["owner_addresses"] = list(set(self.data["owner_addresses"]))
        logging.info("Found {} owner addresses".format(len(self.data["owner_addresses"])))

    def run(self):
        self.get_all_owner_addresses()
        self.get_all_ens()

        self.save_data()
        self.save_metadata()


if __name__ == "__main__":

    scraper = EnsScraper()
    scraper.run()
