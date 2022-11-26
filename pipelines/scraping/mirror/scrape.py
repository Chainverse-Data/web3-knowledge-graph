import requests
import os
from tqdm import tqdm
from .helpers.arweave import MirrorScraperHelper
from ..helpers import Scraper
import logging
import pandas as pd
import web3
import re
from collections import Counter

class MirrorScraper(Scraper):
    def __init__(self):
        super().__init__("mirror")
        self.optimism_start_block = self.metadata.get("optimism_start_block", 8557803)
        content = self.get_request("https://api-optimistic.etherscan.io/api?module=proxy&action=eth_blockNumber&apikey=" + os.environ.get("OPTIMISTIC_ETHERSCAN_API_KEY", ""), decode=False, json=True)
        self.optimism_end_block = int(content["result"], 16)
        self.start_block = self.metadata.get("start_block", 550000)
        self.end_block = self.get_request("https://arweave.net/info", decode=False, json=True)["blocks"]
        self.arweave_url = "https://arweave.net/{}"
        self.arweaveHelpers = MirrorScraperHelper()
        self.ensSearchURL = "https://eth-mainnet.g.alchemy.com/nft/v2/{}/getNFTs?owner={}&contractAddresses[]=0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85&withMetadata=true"
        self.reverse_ens = {}
        self.mirror_NFT_factory_address = "0x302f746eE2fDC10DDff63188f71639094717a766"
        self.writing_editions_address = "0xfd8077F228E5CD9dED1b558Ac21F98ECF18f1a28"
        self.etherescan_API_url = "https://api-optimistic.etherscan.io/api?module=account&action=txlistinternal&address={}&startblock={}&endblock={}&page={}&offset={}&sort=asc&apikey=" + os.environ.get("OPTIMISTIC_ETHERSCAN_API_KEY", "")
        self.etherescan_ABI_API_url = "https://api-optimistic.etherscan.io/api?module=contract&action=getabi&address={}&apikey=" + os.environ.get("OPTIMISTIC_ETHERSCAN_API_KEY", "")
        self.w3 = web3.Web3(web3.HTTPProvider('https://mainnet.optimism.io'))

    def ENSsearch(self, address):
        url = self.ensSearchURL.format(os.environ["ALCHEMY_API_KEY"], address)
        content = self.get_request(url, decode=False, json=True)
        if len(content["ownedNfts"] > 0):
            return content["ownedNfts"][0]["title"]
        return None

    def get_article(self, item):
        url = self.arweave_url.format(item[1])
        content = self.get_request(url, decode=False, json=True)
        return content

    def get_mirror_NFTs(self):
        NFT_addresses = []
        logging.info("Getting all NFTs from Mirror Factory")
        page = 1
        offset = 10000           
        while page:
            url = self.etherescan_API_url.format(self.mirror_NFT_factory_address, self.optimism_start_block, self.optimism_end_block, page, offset)
            transactions = self.get_request(url, decode=False, json=True)
            if transactions["status"] == 1:
                for transaction in transactions:
                    if transaction["type"] == "create2":
                        tmp = {
                            "address": transaction["contractAddress"],
                            "block": transaction["blockNumber"],
                            "timestamp": transaction["timeStamp"],
                            "hash": transaction["hash"]
                        }
                        NFT_addresses.append(tmp)
                page += 1
            else:
                if transactions["message"] == "No transactions found":
                    page = None
        self.data["factory_NFTs"] = NFT_addresses

    def reconcile_NFTs(self):
        arweave_NFTs = set([el["address"] for el in self.data["arweave_nfts"]])
        factory_NFTs = set([el["address"] for el in self.data["factory_NFTs"]])
        articles = set([el["original_content_digest"] for el in self.data["arweave_articles"]])
        contracts_to_check = factory_NFTs.difference(arweave_NFTs)
        url = self.etherescan_ABI_API_url.format(self.writing_editions_address)
        abi = self.get_request(url, decode=False, json=True)["result"]
        missing_NFTs = []
        missing_articles = []
        logging.info("Reconciling NFTs and articles")
        for address in tqdm(contracts_to_check, disabled=self.isAirflow != False):
            contract = self.w3.eth.contract(address=address, abi=abi)
            mirror_url = contract.functions.description().call()
            funding_recipient = contract.functions.fundingRecipient().call()
            supply = contract.functions.limit().call()
            owner = contract.functions.owner().call()
            symbol = contract.functions.symbol().call()
            req = requests.get(mirror_url)
            digest = req.url.split("/")[-1]

            nft = {
                "original_content_digest": digest,
                "chain_id": 10,
                "funding_recipient": funding_recipient,
                "owner": owner,
                "address": address,
                "supply": supply,
                "symbol": symbol,
            }

            missing_NFTs.append(nft)

            if digest not in articles:
                arweave_hash = contract.functions.contentURI().call()
                data = self.get_article(arweave_hash)
                if data["authorship"]["contributor"] not in self.reverse_ens:
                    ens = self.ENSsearch(data["authorship"]["contributor"])
                else:
                    ens = self.reverse_ens[data["authorship"]["contributor"]]
                article = {
                    "original_content_digest": digest,
                    "current_content_digest": digest,
                    "arweaveTx": arweave_hash,
                    "body": data["content"]["body"],
                    "title": data["content"]["title"],
                    "timestamp": data["content"]["timestamp"],
                    "author": data["authorship"]["contributor"],
                    "ens": ens,
                }

                missing_articles.append(article)
        self.data["NFTs"] = self.data["arweave_nfts"] + missing_NFTs
        self.data["articles"] = self.data["arweave_articles"] + missing_articles

    def get_mirror_articles(self):
        logging.info(f"Getting data from blocks: {self.start_block} to {self.end_block}")

        transactions = self.arweaveHelpers.getArweaveTxs(self.start_block, self.end_block, 400)

        transactions_cleaned = []
        articles_cleaned = []
        NFTs_cleaned = []
        done = set()
        logging.info(f"Getting all new transactions")
        for transaction in tqdm(transactions, disabled=self.isAirflow != False):
            tmp = {
                "transaction_id": transaction["node"]["id"],
                "author": transaction["tags"][2]["value"],
                "content-digest": transaction["tags"][3]["value"],
                "original_content_digest": transaction["tags"][4]["value"],
                "block": transaction["block"]["height"],
                "timestamp": transaction["block"]["timestamp"],
            }
            if tmp["transaction_id"] not in done:
                transactions_cleaned.append(tmp)
                done.add(tmp["transaction_id"]) 
        transaction_df = pd.DataFrame.from_dict(transactions_cleaned)
        logging.info(f"Retrieved {len(transaction_df)} transactions")

        logging.info(f"Reverse authors lookup")
        unique_authors = transaction_df["author"].unique()
        for author in tqdm(unique_authors, disabled=self.isAirflow != False):
            ens = self.ENSsearch(author)
            if ens:
                self.reverse_ens[author] = ens
            else:
                self.reverse_ens[author] = ""

        filtered_transactions = transaction_df.sort_values("block").groupby("original_content_digest", as_index=False).head(1)
        logging.info(f"Getting all the articles content")
        for transaction in tqdm(filtered_transactions.to_dict(), disabled=self.isAirflow != False):
            data = self.get_article(transaction["transaction_id"])
            article = {
                "original_content_digest": transaction["original-content-digest"],
                "current_content_digest": transaction["content-digest"],
                "arweaveTx": transaction["transaction_id"],
                "body": data["content"]["body"],
                "title": data["content"]["title"],
                "timestamp": data["content"]["timestamp"],
                "author": transaction["author"],
                "ens": self.reverse_ens[transaction["author"]],
            }
            articles_cleaned.append(article)
            if "wnft" in data:
                nft = {
                    "original_content_digest": transaction["original-content-digest"],
                    "chain_id": data["chainId"],
                    "funding_recipient": data["fundingRecipient"],
                    "owner": data["owner"],
                    "address": data["proxyAddress"],
                    "supply": data["supply"],
                    "symbol": data["symbol"]
                }
                NFTs_cleaned.append(nft)
        logging.info(f"Retrieved {len(articles_cleaned)} articles")

        self.data["arweave_transactions"] = transactions_cleaned
        self.data["arweave_articles"] = articles_cleaned
        self.data["arweave_nfts"] = NFTs_cleaned

    def get_twitter_accounts(self):
        twitter_accounts = []
        for article in tqdm(self.data["articles"], disabled=self.isAirflow != False):
            twitter_account_list = re.findall("twitter.com\/[\w]+", article["body"])
            accounts = [account.split("/")[-1] for account in twitter_account_list]
            counter = Counter(accounts)
            for account in zip(counter.keys(), counter.values()):
                tmp = {
                    "original_content_digest": article["original_content_digest"],
                    "twitter_handle": account[0],
                    "mention_count": account[1]
                } 
                twitter_accounts.append(tmp)
        self.data["twitter_accounts"] = twitter_accounts

    def run(self):
        self.get_mirror_articles()
        self.get_mirror_NFTs()
        self.reconcile_NFTs()
        self.get_twitter_accounts()
        self.save_data()
        self.metadata["start_block"] = self.end_block
        self.metadata["optimism_start_block"] = self.optimism_start_block
        self.save_metadata()

if __name__ == '__main__':
    scrapper = MirrorScraper()
    scrapper.run()