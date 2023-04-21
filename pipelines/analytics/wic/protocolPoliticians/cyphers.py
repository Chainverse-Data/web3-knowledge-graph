from .. import WICCypher
from ....helpers import count_query_logging, get_query_logging


class ProtocolPoliticiansCyphers(WICCypher):
    def __init__(self, subgraph_name, conditions, database=None):
        WICCypher.__init__(self, subgraph_name, conditions, database)

    @count_query_logging
    def connect_voters(self, context): ##FocusedVoter
        ### I would like to revisit this after we do network enrichment for ML. 
        ### I/e maybe we need to add VOTED edges between wallets and entities and put count on the edge
        query = f"""
        MATCH (w:Wallet)-[r:VOTED]->(p:Proposal)-[:HAS_PROPOSAL]-(e:Entity)
        MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
        WITH w, count(distinct(p)) AS votes, wic
        WHERE votes > 10
        MERGE (w)-[con:_HAS_CONTEXT]->(wic)
        SET con.toRemove = null
        SET con._count = votes
        RETURN count(distinct(w))
        """
        count = self.query(query)[0].value()
        return count 

    def get_proposal_authors_benchmark(self):
        engaged_benchmark_query = """
            MATCH (w:Wallet)-[r:AUTHOR]->(p:Proposal)-[:HAS_PROPOSAL]-(e:Entity)
            WITH w, count(distinct(p)) as authored
            RETURN tofloat(apoc.agg.percentiles(authored, [.5])[0])
        """
        engaged_benchmark = self.query(engaged_benchmark_query)[0].value()
        return engaged_benchmark

    @count_query_logging
    def connect_proposal_author(self, context, benchmark): ##FocusedProposalAuthor
        engaged_query = f"""
            WITH tofloat({benchmark}) AS engaged_benchmark
            MATCH (w:Wallet)-[r:AUTHOR]->(p:Proposal)-[:HAS_PROPOSAL]-(e:Entity)
            MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            WITH w, count(distinct(p)) AS authored, engaged_benchmark, wic
            WITH w, wic, (tofloat(authored) / engaged_benchmark) AS againstBenchmark
            MERGE (w)-[con:_HAS_CONTEXT]->(wic)
            SET con.toRemove = null
            SET con._againstBenchmark = againstBenchmark
            RETURN count(distinct(w))
        """
        count = self.query(engaged_query)[0].value()
        return count 

    @count_query_logging
    def connect_delegates(self, context): 
        delegates = f"""
            MATCH (delegator:Wallet)-[:DELEGATES_TO]->(delegate:Wallet)
            WHERE id(delegator) <> id(delegate)
            MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            WITH delegate, wic, count(distinct(delegator)) AS delegators_count
            MERGE (delegate)-[con:_HAS_CONTEXT]->(wic)
            SET con.toRemove = null
            SET con._count = delegators_count
            RETURN count(distinct(delegate))
        """
        count = self.query(delegates)[0].value()
        return count 

    @count_query_logging
    def connect_dao_admins(self, context):
        query = f"""
            MATCH (w:Wallet)-[r:CONTRIBUTOR]->(i:Entity)
            MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            WITH w,wic, count(distinct(i)) AS contributing
            MERGE (w)-[con:_HAS_CONTEXT]->(wic)
            SET con.toRemove = null
            SET con._count = contributing
            RETURN count(distinct(w))
        """
        count = self.query(query)[0].value()
        return count



