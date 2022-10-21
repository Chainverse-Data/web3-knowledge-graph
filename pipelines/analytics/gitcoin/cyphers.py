from ...helpers import Cypher
from ...helpers import get_query_logging

class GitCoinAnalyticsCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    def create_constraints(self):
        pass

    def create_indexes(self):
        pass

    @get_query_logging
    def get_grants_donations_graph(self):
        query = """
            MATCH (grant:Grant)-[donation:DONATION]-(wallet:Wallet)
            RETURN ID(grant), labels(grant), grant.tags, grant.types, ID(wallet), labels(wallet)
        """
        result = self.query(query)
        return result

    def create_donation_subgraph(self):
        """Creates a donation subgraph as a bipartite between donators and grants"""
        pass

    def project_donation_subgraph(self):
        """Project the donation subgraph to create a donators graph for community detections"""
        pass

    def save_donators_communities(self):
        """Save the donators communities"""
        pass
