import networkx as nx
import logging
from tqdm import tqdm
from ..helpers import Analysis, Networks
from .cyphers import GitCoinAnalyticsCyphers



class GitCoinAnalysis(Analysis):
    def __init__(self):
        self.cyphers = GitCoinAnalyticsCyphers()
        super().__init__("gitcoin-communities")
        self.networks = Networks()
        self.grants_weight_threshold = 1
        self.donors_weight_threshold = 1

    def run(self):
        self.create_grant_donor_graph()
        self.grants_partitions()
        self.donors_partitions()
        
    def grants_partitions(self):
        partitionTarget = "GitcoinGrant:Grant"
        self.cyphers.clear_partitions(partitionTarget)
        grants_partitions, grants_labels = self.create_grants_communities()
        data = self.prepare_partitions_data(grants_partitions, grants_labels, partitionTarget)
        
        urls = self.save_json_as_csv(data["labels"], f"grants_partitions_labels_{self.asOf}")
        self.cyphers.create_or_merge_partitions(urls)
        
        urls = self.save_json_as_csv(data["partitions"], f"grants_partitions_{self.asOf}")
        self.cyphers.link_partitions(urls, partitionTarget, "id")
        
    def donors_partitions(self):
        partitionTarget = "Wallet"
        self.cyphers.clear_partitions(partitionTarget)
        donors_partitions, donors_labels = self.create_donors_communities()
        data = self.prepare_partitions_data(donors_partitions, donors_labels, partitionTarget)
        
        urls = self.save_json_as_csv(data["labels"], f"donors_partitions_labels_{self.asOf}")
        self.cyphers.create_or_merge_partitions(urls)
        
        urls = self.save_json_as_csv(data["partitions"], f"donors_partitions_{self.asOf}")
        self.cyphers.link_partitions(urls, partitionTarget, "address")
        
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
    
    def create_grants_communities(self):
        logging.info("Creating grants communities...")
        grants_adj = self.networks.compute_projection(self.biadjacency, self.grants_weight_threshold, axis=0)
        partitions, labels = self.networks.get_partitions(grants_adj, self.grants_id_map)
        return partitions, labels

    def create_donors_communities(self):
        logging.info("Creating donors communities...")
        donors_adj = self.networks.compute_projection(self.biadjacency, self.donors_weight_threshold, axis=1)
        partitions, labels = self.networks.get_partitions(donors_adj, self.donors_id_map)
        return partitions, labels
        
    def create_grant_donor_graph(self):
        logging.info("Creating grant - donors graph")
        result = self.cyphers.get_grants_donations_graph()
        self.G = nx.Graph()

        for record in tqdm(result):
            self.G.add_node(record[0], labels=record[1], tags=record[2], types=record[3])
            self.G.add_node(record[4], labels=record[5])
            self.G.add_edge(record[0], record[4])
        
        self.donors = [n for n in self.G.nodes() if len(set(self.G.nodes[n]["labels"]).intersection(set(["Wallet", "MultiSig"]))) > 0 and self.G.degree[n] > 1]
        self.grants = [n for n in self.G.nodes() if len(set(self.G.nodes[n]["labels"]).intersection(set(["GitcoinGrant", "Grant"]))) > 0 and self.G.degree[n] > 1]

        self.biadjacency, self.grants_id_map, self.donors_id_map = self.networks.compute_biadjacency(self.G, self.grants, self.donors)
        
if __name__ == '__main__':
    analytics = GitCoinAnalysis()
    analytics.run()