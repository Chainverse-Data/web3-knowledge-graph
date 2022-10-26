from ...helpers import Queries
from ...helpers import get_query_logging, count_query_logging
from ...helpers import Cypher

class WalletCommunityAnalyticsCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)
        self.queries = Queries()

    def create_constraints(self):
        pass

    def create_indexes(self):
        pass

    @get_query_logging
    def get_all_relationships(self):
        query = "CALL db.relationshipTypes()"
        results = self.query(query)
        return [el["relationshipType"] for el in results]

    @get_query_logging
    def get_wallets_from_match(self, matchQuery):
        "matchQuery MUST match against Wallets, and the variable must be wallet. example (n)-[r]-(wallet:Wallet)"
        relationships = "|".join(
            [el for el in self.get_all_relationships() if el != "HAS_PARTITION"])
        query = """
        {}
        {}
        WHERE NOT w1 = w2 AND NOT w1:MultiSig AND NOT w2:MultiSig
        MATCH shortestPath((w1)-[r:{}*..4]-(w2))
        RETURN w1.address, w2.address
        """.format(matchQuery.replace("wallet", "w1"), matchQuery.replace("wallet", "w2"), relationships)
        results = self.query(query)
        return results

    @count_query_logging
    def create_or_merge_partitions(self, urls, label):
        count = self.queries.create_or_merge_partitions(urls, label)
        return count

    @count_query_logging
    def link_partitions(self, urls, partitionTarget, targetField, label):
        count = self.queries.link_partitions(
            urls, partitionTarget, targetField, label)
        return count
