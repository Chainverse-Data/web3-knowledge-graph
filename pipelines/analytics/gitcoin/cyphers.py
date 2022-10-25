from ...helpers import Cypher
from ...helpers import get_query_logging, count_query_logging

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
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS partitions
                MERGE(partition:Partition:Gitcoin {{partitionTarget: partitions.partitionTarget, partition: partitions.partition}})
                ON CREATE set partition.uuid = apoc.create.uuid(),
                    partition.asOf = partitions.asOf,
                    partition.method = partitions.method,
                    partition.partition = partitions.partition,
                    partition.partitionTarget = partitions.partitionTarget,
                    partition.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    partition.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    partition.ingestedBy = "{self.CREATED_ID}"
                ON MATCH set partition.partition = partitions.partition,
                    partition.asOf = partitions.asOf,
                    partition.method = partitions.method,
                    partition.partitionTarget = partitions.partitionTarget,
                    partition.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    partition.ingestedBy = "{self.UPDATED_ID}"
                return count(partition)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_partitions(self, urls, partitionTarget, targetField):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS partitions
                MATCH (target:{partitionTarget} {{ {targetField}: partitions.targetField }}), (partition:Partition:Gitcoin {{partitionTarget: "{partitionTarget}", partition: partitions.partition }})
                WITH target, partition, partitions
                MERGE (target)-[link:HAS_PARTITION]->(partition)
                ON CREATE set link.asOf = partitions.asOf,
                    link.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    link.ingestedBy = "{self.CREATED_ID}"
                ON MATCH set link.asOf = partitions.asOf,
                    link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    link.ingestedBy = "{self.UPDATED_ID}"
                RETURN count(link)
            """
            count += self.query(query)[0].value()
        return count
