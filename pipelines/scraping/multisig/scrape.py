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
        self.interval = 1000

    def get_multisig(self):
        cutoff_timestamp = 0
        if "last_timestamp" in self.metadata:
            cutoff_timestamp = int(self.metadata["last_timestamp"])
        skip = 0

        transport = AIOHTTPTransport(url=self.graph_url)
        client = gql.Client(transport=transport, fetch_schema_from_transport=True)

        self.data["multisig"] = []
        while True:
            variables = {"first": self.interval, "skip": skip, "cutoff": cutoff_timestamp}
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
                    }"""
            )
            try:
                result = client.execute(query, variable_values=variables)
                result = result["wallets"]
                if len(result) == 0:
                    break
                for entry in result:
                    for owner in entry["owners"]:
                        self.data["multisig"].append(
                            {
                                "multisig": entry["id"].lower(),
                                "address": owner.lower(),
                                "threshold": int(entry["threshold"]),
                                "occurDt": int(entry["stamp"]),
                            }
                        )
            except Exception as e:
                print(e)

            skip += self.interval

        print("Found {} multisig transactions".format(len(self.data["multisig"])))

    def run(self):
        self.get_multisig()
        self.metadata["last_timestamp"] = int(self.runtime.timestamp())
        self.save_metadata()
        self.save_data()


if __name__ == "__main__":
    scraper = MultisigScraper()
    scraper.run()
