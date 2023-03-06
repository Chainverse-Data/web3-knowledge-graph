from .. import WICCypher
from ....helpers import count_query_logging

class CreatorsCollectorsCypher(WICCypher):
    def __init__(self, subgraph_name, conditions, database=None):
        WICCypher.__init__(self, subgraph_name, conditions, database)

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
            WITH distinct(author) as authors, (tofloat(articles_count) / benchmark) AS againstBenchmark, wic
            MERGE (authors)-[edge:_HAS_CONTEXT]->(wic)
            SET edge._againstBenchmark = againstBenchmark
            RETURN count(edge)
        """
        count = self.query(connect_writers)[0].value()
        return count 
    
    def get_bluechip_benchmark(self, addresses):
        ## ideally we would cover the addresses w/token in context, but this can be changed later

        benchmark_query = f"""
            MATCH (w:Wallet)-[r:HOLDS]->(t:Token)
            WHERE t.contractAddress IN {addresses}
            WITH w, count(distinct(t)) AS collections
            RETURN apoc.agg.percentiles(collections, [.1])[0] AS benchmark
        """
        benchmark = self.query(benchmark_query)[0].value()
        return benchmark

    @count_query_logging
    def cc_blue_chip(self, addresses, context, benchmark):
        connect = f"""
        WITH tofloat({benchmark}) AS benchmark
        MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
        MATCH (w:Wallet)-[r:HOLDS]->(t:Token)
        WHERE (t.address IN {addresses} OR t.contractAddress IN {addresses})
        WITH w, wic, count(distinct(t)) AS collections_held, benchmark
        WITH w, wic, collections_held, benchmark, (tofloat(collections_held) / benchmark) AS againstBenchmark
        MERGE (w)-[r:_HAS_CONTEXT]->(wic)
        SET r._count = collections_held
        SET r._againstBenchmark = againstBenchmark
        RETURN count(distinct(w)) AS count
        """
        count = self.query(connect)[0].value()
        return count 
    
    def three_letter_ens(self, context):
        query = f"""
        match 
            (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
        match 
            (wallet:Wallet)-[:HAS_ALIAS]-(alias:Alias:Ens)
        with 
            wallet, split(alias.name, ".eth")[0] as ens_name, wic
        where 
            size(ens_name) = 3 
        with 
            wallet, wic 
        merge
            (wallet)-[con:_HAS_CONTEXT]->(wic)
        return
            count(con)
        """
        count = self.query(query)[0].value() 

        return count
    
    def sudo_power_users(self, urls):
        count = 0
        for url in urls:
            create_wallets = f"""
            load csv with headers from '{url}' as sudo
            merge (wallet:Wallet {{address: sudo.address}})
            return count(wallet)
            """
            count += self.query(create_wallets)[0].value()

        return count

    def connect_sudo_power_users(self, context, urls):
        count = 0
        for url in urls: 
            query = f"""
            load csv with headers from '{url}' as sudo
            match (wallet:Wallet {{address: sudo.address}})
            match (wic:_Wic:_{self.subgraph_name}:_{context})
            with wallet, wic
            merge (wallet)-[r:_HAS_CONTEXT->(wic)
            set r._context = {{round(tofloat(sudo.total_volume), 4)}}
            return count(*)
            """
            count += self.query(query)[0].value()

        return count 
        
        

