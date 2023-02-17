import multiprocessing
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
        self.blockStep = 400
        self.arweaveHelpers = MirrorScraperHelper(self.blockStep)
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
            transactions = self.get_request(url, decode=False, json=True)
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
                funding_recipient = contract.functions.fundingRecipient().call()
                supply = contract.functions.limit().call()
                owner = contract.functions.owner().call()
                symbol = contract.functions.symbol().call()
                req = requests.get(mirror_url, verify=False)
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

                if digest not in articles and digest != address:
                    arweave_hash = contract.functions.contentURI().call()
                    if (arweave_hash.strip()):
                        data = self.get_article(arweave_hash)
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

        transactions = self.arweaveHelpers.getArweaveTxs(self.start_block, self.end_block)

        transactions_cleaned = []
        done = set()
        logging.info(f"Getting all new transactions")
        for transaction in tqdm(transactions):
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

        filtered_transactions = transaction_df.sort_values("block").groupby("original_content_digest", as_index=False).head(1)
        logging.info(f"Getting all the articles content")
        
        articles_content = self.parallel_process(self.get_article_content, filtered_transactions.to_dict('records'), description="Getting all articles content")
        articles_cleaned = [element[0] for element in articles_content if element[0]]
        NFTs_cleaned = [element[1] for element in articles_content if element[1]]

        logging.info(f"Retrieved {len(articles_cleaned)} articles")

        self.data["arweave_transactions"] = transactions_cleaned
        self.data["arweave_articles"] = articles_cleaned
        self.data["arweave_nfts"] = NFTs_cleaned

    def get_article_content(self, transaction):
        data = self.get_article(transaction["transaction_id"])
        article = None
        nft = None
        if data and type(data) == dict and "content" in data:
            if "original_content_digest" in transaction:
                content = data.get("content", {"body": "", "title":"", "timestamp": 0})
                article = {
                    "original_content_digest": transaction.get("original_content_digest", ""),
                    "current_content_digest": transaction.get("content_digest", ""),
                    "arweaveTx": transaction.get("transaction_id", "0x0"),
                    "body": content["body"],
                    "title": content["title"],
                    "timestamp": content["timestamp"],
                    "author": transaction.get("author", "0x0"),
                }
                if "wnft" in data:
                        nft = {
                            "original_content_digest": transaction.get("original_content_digest", ""),
                            "chain_id": data.get("chainId", -1),
                            "funding_recipient": data.get("fundingRecipient", "0x0"),
                            "owner": data.get("owner", "0x0"),
                            "address": data.get("proxyAddress", "0x0"),
                            "supply": data.get("supply", 0),
                            "symbol": data.get("symbol", "")
                        }
            else:
                logging.error("original_content_digest is missing from the transaction, can't resolve article")
        else:
            logging.error("An error occured retriving articles")
        return (article, nft)

    def get_twitter_accounts(self):
        twitter_accounts = []
        for article in tqdm(self.data["articles"]):
            twitter_account_list = re.findall("twitter.com\/[\w]+", article["body"])
            accounts = [account.split("/")[-1] for account in twitter_account_list]
            counter = Counter(accounts)
            for account in zip(counter.keys(), counter.values()):
                if account and len(account) > 1:
                    tmp = {
                        "original_content_digest": article.get("original_content_digest", ""),
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