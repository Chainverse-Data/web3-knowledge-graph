from ..helpers import Analysis
from .cyphers import GitCoinAnalyticsCyphers
import networkx as nx
from networkx.algorithms import bipartite

class GitCoinAnalysis(Analysis):
    def __init__(self):
        super().__init__()
        self.cyphers = GitCoinAnalyticsCyphers()

    def create_donators_communities(self):
        G = self.create_donators_subgraph()

    def create_donators_subgraph(self):
        result = self.cyphers.get_grants_donations()
        G = nx.Graph()



