from ...helpers import Cypher
from ...helpers import get_query_logging, count_query_logging
from ...helpers import Queries

class GitCoinAnalyticsCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)
        self.queries = Queries()

    def create_constraints(self):
        pass

    def create_indexes(self):
        pass

    @get_query_logging
    def get_grants_donations_graph(self):
        query = """
            MATCH (grant:Grant)-[donation:DONATION]-(wallet:Wallet)
            RETURN grant.id, labels(grant), grant.tags, grant.types, wallet.address, labels(wallet)
        """
        result = self.query(query)
        return result

    @count_query_logging
    def clear_partitions(self, partitionTarget):
        query = f"""
            MATCH (partition:Partition:Gitcoin {{partitionTarget: "{partitionTarget}"}})
            DETACH DELETE partition
            RETURN count(partition)
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_partitions(self, urls):
        count = self.queries.create_or_merge_partitions(urls, "Gitcoin")
        return count

    @count_query_logging
    def link_partitions(self, urls, partitionTarget, targetField):
        count = self.queries.link_partitions(urls, partitionTarget, targetField,  "Gitcoin")
        return count
