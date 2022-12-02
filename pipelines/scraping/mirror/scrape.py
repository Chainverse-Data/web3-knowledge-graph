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
DEBUG = os.environ.get("DEBUG", False)

class MirrorScraper(Scraper):
    def __init__(self, bucket_name="mirror", allow_override=False):
        super().__init__(bucket_name, allow_override=allow_override)
        self.optimism_start_block = self.metadata.get("optimism_start_block", 8557803)
        content = self.get_request("https://api-optimistic.etherscan.io/api?module=proxy&action=eth_blockNumber&apikey=" + os.environ.get("OPTIMISTIC_ETHERSCAN_API_KEY", ""), decode=False, json=True)
        self.optimism_end_block = int(content["result"], 16)
        if DEBUG:
            self.optimism_start_block = 8557803
            self.optimism_end_block = 8567803
        self.start_block = self.metadata.get("start_block", 595567)
        self.end_block = self.get_request("https://arweave.net/info", decode=False, json=True)["blocks"]
        if DEBUG:
            self.start_block = 595567
            self.end_block = 599567
        self.arweave_url = "https://arweave.net/{}"
        self.arweaveHelpers = MirrorScraperHelper()
        self.ensSearchURL = "https://eth-mainnet.g.alchemy.com/nft/v2/{}/getNFTs?owner={}&contractAddresses[]=0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85&withMetadata=true"
        self.reverse_ens = {}
        self.mirror_NFT_factory_address = "0x302f746eE2fDC10DDff63188f71639094717a766"
        self.writing_editions_address = "0xfd8077F228E5CD9dED1b558Ac21F98ECF18f1a28"
        self.etherescan_API_url = "https://api-optimistic.etherscan.io/api?module=account&action=txlistinternal&address={}&startblock={}&endblock={}&page={}&offset={}&sort=asc&apikey=" + os.environ.get("OPTIMISTIC_ETHERSCAN_API_KEY", "")
        self.etherescan_ABI_API_url = "https://api-optimistic.etherscan.io/api?module=contract&action=getabi&address={}&apikey=" + os.environ.get("OPTIMISTIC_ETHERSCAN_API_KEY", "")
        self.alchemy_optimism_rpc = f"https://opt-mainnet.g.alchemy.com/v2/{os.environ['ALCHEMY_API_KEY']}"
        self.w3 = web3.Web3(web3.HTTPProvider(self.alchemy_optimism_rpc))

    def ENSsearch(self, address):
        url = self.ensSearchURL.format(os.environ["ALCHEMY_API_KEY"], address)
        content = self.get_request(url, decode=False, json=True)
        if len(content.get("ownedNfts", 0)) > 0:
            return content["ownedNfts"][0]["title"]
        return None

    def get_article(self, arweaveHash):
        url = self.arweave_url.format(arweaveHash)
        print(url)
        content = self.get_request(url, decode=False, json=True)
        return content

    def get_mirror_NFTs(self):
        NFT_addresses = []
        logging.info("Getting all NFTs from Mirror Factory")
        page = 1
        offset = 10000           
        while page:
            logging.info(f"Scrapping ... currently got {len(NFT_addresses)} NFTs from Optimism...")
            url = self.etherescan_API_url.format(self.mirror_NFT_factory_address, self.optimism_start_block, self.optimism_end_block, page, offset)
            print(url)
            transactions = self.get_request(url, decode=False, json=True)
            print(transactions)
            if transactions["status"] == '1':
                for transaction in transactions["result"]:
                    if transaction["type"] in ["create2", "create"]:
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
        for address in tqdm(contracts_to_check):
            logging.info(f"Getting informations from NFT at: {address}")
            try:
                contract = self.w3.eth.contract(address=self.w3.toChecksumAddress(address), abi=abi)
                mirror_url = contract.functions.description().call()
                print(mirror_url)
                funding_recipient = contract.functions.fundingRecipient().call()
                print(funding_recipient)
                supply = contract.functions.limit().call()
                print(supply)
                owner = contract.functions.owner().call()
                print(owner)
                symbol = contract.functions.symbol().call()
                print(symbol)
                req = requests.get(mirror_url, verify=False)
                print(req.url)
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
                print(nft)
                missing_NFTs.append(nft)

                if digest not in articles and digest != address:
                    arweave_hash = contract.functions.contentURI().call()
                    print("hash", arweave_hash)
                    if (arweave_hash.strip()):
                        data = self.get_article(arweave_hash)
                        print("data", data)
                        # if data["authorship"]["contributor"] not in self.reverse_ens:
                        #     ens = self.ENSsearch(data["authorship"]["contributor"])
                        # else:
                        #     ens = self.reverse_ens[data["authorship"]["contributor"]]
                        article = {
                            "original_content_digest": digest,
                            "current_content_digest": digest,
                            "arweaveTx": arweave_hash,
                            "body": data["content"]["body"],
                            "title": data["content"]["title"],
                            "timestamp": data["content"]["timestamp"],
                            "author": data["authorship"]["contributor"],
                            # "ens": ens,
                        }

                        missing_articles.append(article)
            except:
                logging.warning(f"Contract: {address} is probably not a Mirror NFT")
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
        for transaction in tqdm(transactions):
            print(transaction)
            author, content_digest, original_content_digest = None, None, None
            for tag in transaction["node"]["tags"]:
                if tag["name"] == "Contributor":
                    author = tag["value"]
                if tag["name"] == "Content-Digest":
                    content_digest = tag["value"]
                if tag["name"] == "Original-Content-Digest":
                    original_content_digest = tag["value"]
            if not original_content_digest:
                original_content_digest = content_digest
            tmp = {
                "transaction_id": transaction["node"]["id"],
                "author": author,
                "content_digest": content_digest,
                "original_content_digest": original_content_digest,
                "block": transaction["node"]["block"]["height"],
                "timestamp": transaction["node"]["block"]["timestamp"],
            }
            if tmp["transaction_id"] not in done:
                transactions_cleaned.append(tmp)
                done.add(tmp["transaction_id"]) 
        transaction_df = pd.DataFrame.from_dict(transactions_cleaned)
        logging.info(f"Retrieved {len(transaction_df)} transactions")

        logging.info(f"Reverse authors lookup")
        unique_authors = transaction_df["author"].unique()
        # for author in tqdm(unique_authors):
        #     ens = self.ENSsearch(author)
        #     if ens:
        #         self.reverse_ens[author] = ens
        #     else:
        #         self.reverse_ens[author] = ""

        filtered_transactions = transaction_df.sort_values("block").groupby("original_content_digest", as_index=False).head(1)
        logging.info(f"Getting all the articles content")
        for transaction in tqdm(filtered_transactions.to_dict('records')):
            print(transaction)
            data = self.get_article(transaction["transaction_id"])
            if data:
                article = {
                    "original_content_digest": transaction["original_content_digest"],
                    "current_content_digest": transaction["content_digest"],
                    "arweaveTx": transaction["transaction_id"],
                    "body": data["content"]["body"],
                    "title": data["content"]["title"],
                    "timestamp": data["content"]["timestamp"],
                    "author": transaction["author"],
                    # "ens": self.reverse_ens[transaction["author"]],
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
            else:
                logging.error("An error occured retriving articles")
        logging.info(f"Retrieved {len(articles_cleaned)} articles")

        self.data["arweave_transactions"] = transactions_cleaned
        self.data["arweave_articles"] = articles_cleaned
        self.data["arweave_nfts"] = NFTs_cleaned

    def get_twitter_accounts(self):
        twitter_accounts = []
        for article in tqdm(self.data["articles"]):
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