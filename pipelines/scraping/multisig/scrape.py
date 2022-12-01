import time
from ..helpers import Scraper
from ..helpers import str2bool
import os
import argparse
import gql
import logging
from gql.transport.aiohttp import AIOHTTPTransport, log as gql_log

gql_log.setLevel(logging.WARNING)


class MultisigScraper(Scraper):
    def __init__(self):
        super().__init__("multisig", allow_override=bool(os.environ["ALLOW_OVERRIDE"]))
        self.graph_url = (
            "https://gateway.thegraph.com/api/{}/subgraphs/id/3oPKQiPKyD1obYpi5zXBy6HoPdYoDgxXptKrZ8GC3N1N".format(
                os.environ["GRAPH_API_KEY"]
            )
        )
        self.cutoff_timestamp = self.metadata.get("last_timestamp", 0)
        self.interval = 1000
        self.data["multisig"] = []
        self.data["transactions"] = []


    def call_the_graph_api(self, query, variables, counter=0):
        time.sleep(counter)
        if counter > 10:
            return None

        transport = AIOHTTPTransport(url=self.graph_url)
        client = gql.Client(transport=transport,
                            fetch_schema_from_transport=True)
        try:
            result = client.execute(query, variable_values=variables)
            if not result.get("wallets", None):
                logging.error("theGraph API did not return wallets", result, "counter:", counter)
                return self.call_the_graph_api(query, variables, counter=counter+1)
            if not result.get("transactions", None):
                logging.error("theGraph API did not return transactions", result, "counter:", counter)
                return self.call_the_graph_api(query, variables, counter=counter+1)
        except Exception as e:
            logging.error("An exception occurred getting the graph API", e, "counter:", counter)
            return self.call_the_graph_api(query, variables, counter=counter+1)
        return result
        
    def get_multisig_and_transactions(self):
        skip = 0
        wallets = ["init"]
        transactions = ["init"]
        while len(wallets) > 0 and len(transactions) > 0:
            variables = {"first": self.interval, "skip": skip, "cutoff": self.cutoff_timestamp}
            query = gql.gql(
                """query($first: Int!, $skip: Int!, $cutoff: BigInt!) {
                        wallets(first: $first, skip: $skip, orderBy: stamp, orderDirection:desc, where:{stamp_gt: $cutoff}) {
                            id
                            creator
                            network
                            stamp
                            factory
                            owners
                            threshold
                            version
                        }
                        transactions(first: $first, skip: $skip, orderBy: stamp, orderDirection:desc, where:{stamp_gt: $cutoff}) {
                            stamp
                            block
                            hash
                            wallet {
                                id
                            }
                            destination
                        }
                    }"""
            )
            result = self.call_the_graph_api(query, variables)
            wallets = result["wallets"]
            transactions = result["transactions"]
            for wallet in wallets:
                for owner in wallet["owners"]:
                    tmp = {
                        "multisig": wallet["id"],
                        "address": owner,
                        "threshold": int(wallet["threshold"]),
                        "occurDt": int(wallet["stamp"]),
                        "network": wallet["network"],
                        "factory": wallet["factory"],
                        "version": wallet["version"],
                        "creator": wallet["creator"], 
                        "timestamp": wallet["stamp"]
                    }
                    self.data["multisig"].append(tmp)
            for transaction in transactions:
                tmp = {
                    "timestamp": transaction["stamp"],
                    "block": transaction["block"],
                    "from": transaction["wallet"]["id"],
                    "to": transaction["destination"],
                    "txHash": transaction["hash"]
                }
                self.data["transactions"].append(tmp)
            skip += self.interval
            logging.info("Query success, skip is at:", skip)
        logging.info("Found {} multisig and {} transactions".format(
            len(self.data["multisig"])), len(self.data["transactions"]))

    def run(self):
        self.get_multisig_and_transactions()
        self.metadata["last_timestamp"] = int(self.runtime.timestamp())
        self.save_metadata()
        self.save_data()


if __name__ == "__main__":
    scraper = MultisigScraper()
    scraper.run()
