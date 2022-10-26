import networkx as nx
import logging
from tqdm import tqdm
import os
from ..helpers import Analysis, Networks
from .cyphers import WalletCommunityAnalyticsCyphers

# Examples
# os.environ["MATCH_QUERY"] = """MATCH (n:DaoHaus {daohausId: "0x016e79e9101a8eaa3e7f46d6d1c267819c09c939"})-[:CONTRIBUTOR]-(wallet:Wallet)"""
# os.environ["ANALYSIS_NAME"] = "daoTest"

class WalletCommunityAnalysis(Analysis):
    def __init__(self):
        if "ANALYSIS_NAME" not in os.environ:
            raise(ValueError("ANALYSIS_NAME has not been set in ENV!"))
        self.name = os.environ["ANALYSIS_NAME"].lower().capitalize()
        super().__init__(f"{self.name.lower()}-communities")
        self.cyphers = WalletCommunityAnalyticsCyphers()
        self.networks = Networks()
        self.grants_weight_threshold = 1
        self.donors_weight_threshold = 1
        if "MATCH_QUERY" not in os.environ:
            raise(ValueError("MATCH_QUERY has not been set in ENV!"))
        self.matchQuery = os.environ["MATCH_QUERY"]

    def run(self):
        partitionTarget = "Wallet"
        adjacency, wallet_mapping = self.create_wallet_adjacency()
        partitions, labels = self.networks.get_partitions(
            adjacency, wallet_mapping)
        data = self.prepare_partitions_data(
            partitions, labels, partitionTarget)

        urls = self.s3.save_json_as_csv(
            data["labels"], self.bucket_name, f"{self.name}_partitions_labels_{self.asOf}")
        self.cyphers.create_or_merge_partitions(urls, self.name)

        urls = self.s3.save_json_as_csv(
            data["partitions"], self.bucket_name, f"{self.name}_partitions_{self.asOf}")
        self.cyphers.link_partitions(
            urls, partitionTarget, "address", self.name)

    def prepare_partitions_data(self, partitions, labels, partitionTarget):
        logging.info("Preparing grants data...")
        data = {
            "partitions": [],
            "labels": [],
        }
        for label in labels:
            data["labels"].append(
                {
                    "asOf": self.asOf,
                    "method": "Louvain",
                    "partition": int(label),
                    "partitionTarget": partitionTarget
                }
            )
        for node_id in partitions:
            data["partitions"].append(
                {
                    "targetField": node_id,
                    "partition": int(partitions[node_id]),
                    "asOf": self.asOf
                }
            )
        return data

    def create_wallet_adjacency(self):
        logging.info("Creating wallet's projection adjacency matrix")
        results = self.cyphers.get_wallets_from_match(self.matchQuery)
        uniqueWallets = set()
        for result in results:
            uniqueWallets.add(result[0])
            uniqueWallets.add(result[1])
        uniqueWallets = list(uniqueWallets)

        wallet_mapping = {}
        for i in range(len(uniqueWallets)):
            wallet_mapping[uniqueWallets[i]] = i

        adjacency = np.zeros((len(uniqueWallets), len(uniqueWallets)))
        for result in tqdm(results):
            i = wallet_mapping[result[0]]
            j = wallet_mapping[result[1]]
            adjacency[i][j] += 1
            adjacency[j][i] += 1
        logging.info(f"Adjacency created with {len(adjacency)} nodes")
        return adjacency, wallet_mapping

if __name__ == '__main__':
    wca = WalletCommunityAnalysis()
    wca.run()