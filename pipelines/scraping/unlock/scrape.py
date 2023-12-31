import time
from ..helpers import Scraper
import os
import requests
import logging
import json

class UnlockScraper(Scraper):
    def __init__(self, bucket_name="unlock"): 
        super().__init__(bucket_name)

        ## graph urls
        self.graph_urls = [
            {"network": "mainnet", "url": "https://api.thegraph.com/subgraphs/name/unlock-protocol/unlock"},
            {"network": "xdai", "url": "https://api.thegraph.com/subgraphs/name/unlock-protocol/xdai"},
            {"network": "optimism", "url": "https://api.thegraph.com/subgraphs/name/unlock-protocol/optimism"},
            {"network": "arbitrum", "url": "https://api.thegraph.com/subgraphs/name/unlock-protocol/arbitrum"},
        ]
        self.polygon_graph_url = {
            "network": "polygon",
            "url": "https://api.thegraph.com/subgraphs/name/unlock-protocol/polygon-v2",
        }

        self.headers = {"accept": "application/json", "content-type": "application/json"}
        self.polygon_api_url = f"https://polygon-mainnet.g.alchemy.com/v2/{os.environ['ALCHEMY_API_KEY_POLYGON']}"

        self.interval = 1000
        self.data["locks"] = []
        self.data["managers"] = []
        self.data["keys"] = []
        self.data["holders"] = []

    def get_locks(self):
        for url in self.graph_urls:
            graph_url = url["url"]
            network = url["network"]
            logging.info(f"Getting locks for {network} from {graph_url}")
            skip = 0
            cutoff_block = self.metadata.get(f"{network}_cutoff_block", 0)

            while True:
                if skip > 5000:
                    skip = 0
                    cutoff_block = self.data["locks"][-1]["creationBlock"]
                variables = {"first": self.interval, "skip": skip, "cutoff": cutoff_block}

                locks_query = """
                    query($first: Int!, $skip: Int!, $cutoff: BigInt!) {
                        locks(first: $first, skip: $skip, orderBy: creationBlock, orderDirection: asc, where: {creationBlock_gt: $cutoff}) {
                                id
                                address
                                name
                                tokenAddress
                                creationBlock
                                price
                                expirationDuration
                                totalSupply
                                LockManagers {
                                    id
                                    address
                                }
                                keys {
                                    id
                                    keyId
                                    owner { 
                                        id
                                        address
                                    }
                                    expiration
                                    tokenURI
                                    createdAt
                                }
                            }
                        }
                        """
                result = self.call_the_graph_api(graph_url, locks_query, variables, ["locks"])
                if result["locks"] == []:
                    logging.info(f"Finished scraping {network} locks")
                    logging.info(f"Current lock count: {len(self.data['locks'])}")
                    break
                for l in result["locks"]:
                    locks_tmp = {
                        "address": l["address"].lower(),
                        "id": l["id"],
                        "name": l["name"],
                        "tokenAddress": l["tokenAddress"],
                        "creationBlock": l["creationBlock"],
                        "price": l["price"],
                        "expirationDuration": l["expirationDuration"],
                        "totalSupply": l["totalSupply"],
                        "network": network,
                    }
                    self.data["locks"].append(locks_tmp)
                for lc in result["locks"]:
                    for manager in lc["LockManagers"]:
                        managers_tmp = {"lock": lc["address"], "address": manager["address"].lower()}
                        self.data["managers"].append(managers_tmp)
                for l in result["locks"]:
                    address = l["tokenAddress"]
                    for k in l["keys"]:
                        keys_tmp = {
                            "id": k["id"],
                            "address": address,
                            "expiration": k["expiration"],
                            "tokenURI": k["tokenURI"],
                            "createdAt": k["createdAt"],
                            "network": network,
                        }
                        self.data["keys"].append(keys_tmp)

                        holders_tmp = {
                            "id": k["owner"]["id"],
                            "address": k["owner"]["address"].lower(),
                            "keyId": k["id"],
                            "tokenAddress": address,
                        }
                        self.data["holders"].append(holders_tmp)
                self.metadata[f"{network}_cutoff_block"] = self.data["locks"][-1]["creationBlock"]
                skip += self.interval

    def get_polygon_locks(self):
        graph_url = self.polygon_graph_url["url"]
        network = self.polygon_graph_url["network"]
        logging.info(f"Getting locks for {network} from {graph_url}")
        skip = 0
        cutoff_block = self.metadata.get(f"{network}_cutoff_block", 0)
        result = dict()

        while True:
            if skip > 5000:
                skip = 0
                cutoff_block = self.data["locks"][-1]["creationBlock"]
            variables = {"first": self.interval, "skip": skip, "cutoff": cutoff_block}

            locks_query = """
                query($first: Int!, $skip: Int!, $cutoff: BigInt!) {
                    locks(first: $first, skip: $skip, orderBy: createdAtBlock, orderDirection: asc, where: {createdAtBlock_gt: $cutoff}) {
                        id
                        address
                        name
                        tokenAddress
                        createdAtBlock
                        price
                        expirationDuration
                        totalKeys
                        lockManagers
                        keys {
                            id
                            owner
                            expiration
                            tokenURI
                            createdAtBlock
                            }
                        }
                    }
                    """
            result = self.call_the_graph_api(graph_url, locks_query, variables, ["locks"])
            if result is not None and "locks" in result and result["locks"] == []:
                logging.info(f"Finished scraping {network} locks")
                logging.info(f"Current lock count: {len(self.data['locks'])}")
                break
            elif result is None:
                logging.error("The Graph API call returned a NoneType result")
                break
            for l in result["locks"]:
                locks_tmp = {
                    "address": l["address"].lower(),
                    "id": l["id"],
                    "name": l["name"],
                    "tokenAddress": l["tokenAddress"],
                    "creationBlock": l["createdAtBlock"],
                    "price": l["price"],
                    "expirationDuration": l["expirationDuration"],
                    "totalSupply": l["totalKeys"],
                    "network": network,
                }
                self.data["locks"].append(locks_tmp)
            for lc in result["locks"]:
                for manager in lc["lockManagers"]:
                    managers_tmp = {"lock": lc["address"], "address": manager.lower()}
                    self.data["managers"].append(managers_tmp)
            for l in result["locks"]:
                address = l["tokenAddress"]
                for k in l["keys"]:
                    payload = {
                        "id": 1,
                        "jsonrpc": "2.0",
                        "method": "eth_getBlockByNumber",
                        "params": [hex(int(k["createdAtBlock"])), False],
                    }
                    response = requests.post(self.polygon_api_url, json=payload, headers=self.headers)
                    stamp = int(json.loads(response.text)["result"]["timestamp"], 16)
                    keys_tmp = {
                        "id": k["id"],
                        "address": address,
                        "expiration": k["expiration"],
                        "tokenURI": k["tokenURI"],
                        "createdAt": stamp,
                        "network": network,
                    }
                    self.data["keys"].append(keys_tmp)

                    holders_tmp = {
                        "address": k["owner"].lower(),
                        "keyId": k["id"],
                        "tokenAddress": address,
                    }
                    self.data["holders"].append(holders_tmp)
            self.metadata[f"{network}_cutoff_block"] = self.data["locks"][-1]["creationBlock"]
            skip += self.interval

    def run(self):
        self.get_locks()
        self.get_polygon_locks()
        self.save_metadata()
        self.save_data()

if __name__ == "__main__":
    scraper = UnlockScraper()
    scraper.run()
    logging.info("Run complete!")
