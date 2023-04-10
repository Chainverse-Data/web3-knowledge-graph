from .. import WICCypher
from ....helpers import Queries, count_query_logging

class CreatorsCypher(WICCypher):
    def __init__(self, subgraph_name, conditions, database=None):
        WICCypher.__init__(self, subgraph_name, conditions, database)
        self.queries = Queries()

    def get_writers_benchmark(self):
        benchmark_query = """
            MATCH (w:Wallet)-[r:AUTHOR]->(a:Article:Mirror)
            WITH w, count(distinct(a)) AS articles
            RETURN apoc.agg.percentiles(articles, [.75])[0] AS benchmark
        """
        benchmark = self.query(benchmark_query)[0].value()
        return benchmark

    @count_query_logging
    def cc_writers(self, context, benchmark):
        connect_writers = f"""
            MATCH (author:Wallet)-[r:AUTHOR]->(article:Article:Mirror)
            MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            WITH author, count(distinct(article)) AS articles_count, tofloat({benchmark}) AS benchmark, wic
            WHERE articles_count >= benchmark
            MERGE (author)-[edge:_HAS_CONTEXT]->(wic)
            SET edge.count = articles_count
            RETURN count(author)
        """
        count = self.query(connect_writers)[0].value()
        return count 
    
    @count_query_logging
    def get_web3_musicians(self, context):
        count = 0 
        ### gets sound.xyz artists
        soundQuery = f"""
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]->(sound:Sound:Account)
        MATCH (context:_Wic:_{self.subgraph_name}:_{context})
        WITH wallet, context
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        RETURN COUNT(DISTINCT(wallet))
        """
        count = self.query(soundQuery)[0].value()

        ## do some silly bio query shit
        queries = ["'recording' AND 'artist'", "'music' AND 'artist'", "'music' AND 'performance'", "'music' AND 'producer'", "'artist' AND 'music'", "'rapper", "'rap artist", "'music' AND 'performance'"]
        for query in queries:
            bioQuery = f"""
            CALL db.index.fulltext.queryNodes("wicBios", "{query}")
            YIELD node
            UNWIND node AS musician
            WITH musician
            MATCH (wallet:Wallet)-[:HAS_ACCOUNT]->(musician)
            WITH wallet
            MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
            MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
            RETURN COUNT(DISTINCT(wallet))
            """
            count += self.query(bioQuery)[0].value()

        return count

    @count_query_logging
    def get_dune_dashboard_wizards(self, context):
        ## folows = stars
        query = f"""
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]->(dune:Dune:Account)
        WITH apoc.agg.percentiles(dune.follows)[2] as cutoff
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]->(dune:Dune:Account)
        WHERE dune.follows > cutoff
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, context
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        RETURN COUNT(DISTINCT(wallet))
        """
        count = self.query(query)[0].value()

        return count 

        
            