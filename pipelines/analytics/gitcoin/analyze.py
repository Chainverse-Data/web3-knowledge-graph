from ..helpers import Analysis
from .cyphers import GitCoinAnalyticsCyphers
import networkx as nx
import logging
import numpy as np
from tqdm import tqdm

class GitCoinAnalysis(Analysis):
    def __init__(self):
        super().__init__()
        self.cyphers = GitCoinAnalyticsCyphers()

        self.grants_weight_threshold = 1
        self.donors_weight_threshold = 1

    def create_donators_communities(self):
        self.create_grant_donor_graph()
        grants_adj, grants_id_map = self.create_grants_projection()

    def create_grant_donor_graph(self):
        logging.info("Creating grant - donors graph")
        result = self.cyphers.get_grants_donations_graph()
        self.G = nx.Graph()

        for record in result:
            self.G.add_node(record[0], label=record[1][-1], tags=record[2], types=record[3])
            self.G.add_node(record[4], label=record[5][-1])
            self.G.add_edge(record[0], record[4])

    def create_grants_projection(self):
        logging.info("Creating grants projection...")
        donors = [n for n in self.G.nodes() if self.G.nodes[n]["label"] in [
            "Wallet", "MultiSig"] and self.G.degree[n] > 1]
        grants = [n for n in self.G.nodes() if self.G.nodes[n]["label"] in [
            "GitcoinGrant", "Gitcoin"] and self.G.degree[n] > 1]

        grants_id_map = {}
        for i in range(len(grants)):
            grants_id_map[grants[i]] = i

        logging.info("Computing adjacency matrix...")
        grants_adj = np.zeros((len(grants), len(grants)), dtype=int)
        for donor in tqdm(donors):
            tmp = [grants_id_map[edge[1]]
                for edge in self.G.edges(donor) if edge[1] in grants_id_map]
            for i in tmp:
                for j in tmp:
                    if i != j:
                        grants_adj[i][j] += 1
        logging.info(f"Filtering weights < {self.grants_weight_threshold}...")
        B = np.zeros((len(grants), len(grants)), dtype=int)
        for i in tqdm(range(len(grants_adj))):
            for j in range(len(grants_adj)):
                if grants_adj[i][j] > self.grants_weight_threshold:
                    B[i][j] = grants_adj[i][j]
        grants_adj = B
        return grants_adj, grants_id_map

    def create_donors_projection(self):
        logging.info("Creating donors projection...")
        donors = [n for n in self.G.nodes() if self.G.nodes[n]["label"] in [
            "Wallet", "MultiSig"] and self.G.degree[n] > 1]
        grants = [n for n in self.G.nodes() if self.G.nodes[n]["label"] in [
            "GitcoinGrant", "Gitcoin"] and self.G.degree[n] > 1]

        donors_id_map = {}
        for i in range(len(donors)):
            donors_id_map[donors[i]] = i

        logging.info("Computing adjacency matrix...")
        donors_adj = np.zeros((len(donors), len(donors)), dtype=int)
        for grant in tqdm(grants):
            tmp = [donors_id_map[edge[1]]
                   for edge in self.G.edges(grant) if edge[1] in donors_id_map]
            for i in tmp:
                for j in tmp:
                    if i != j:
                        donors_adj[i][j] += 1
        logging.info(f"Filtering weights < {self.donors_weight_threshold}...")
        B = np.zeros((len(donors), len(donors)), dtype=int)
        for i in tqdm(range(len(donors_adj))):
            for j in range(len(donors_adj)):
                if donors_adj[i][j] > self.donors_weight_threshold:
                    B[i][j] = donors_adj[i][j]
        donors_adj = B
        return donors_adj, donors_id_map
