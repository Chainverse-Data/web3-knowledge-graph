from .. import WICCypher
from ....helpers import count_query_logging
import logging 

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
            WHERE articles_count >= benchmark
            MERGE (author)-[edge:_HAS_CONTEXT]->(wic)
            SET edge.count = articles_count
            RETURN count(author)
        """
        count = self.query(connect_writers)[0].value()
        return count 
    
    @count_query_logging
    def cc_blue_chip(self, addresses, context):
        connect = f"""
        MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
        MATCH (wallet:Wallet)-[r:HOLDS]->(token:Token)
        WHERE (token.address IN {addresses} OR token.contractAddress IN {addresses})
        WITH wallet, wic, count(distinct(token)) as count_collections
        MERGE (wallet)-[r:_HAS_CONTEXT]->(wic)
        SET r.count = count_collections
        RETURN count(distinct(wallet)) AS count
        """
        count = self.query(connect)[0].value()
        return count 

    @count_query_logging
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
    @count_query_logging
    def create_sudo_power_users(self, urls):
        count = 0
        for url in urls:
            create_wallets = f"""
            load csv with headers from '{url}' as sudo
            merge (wallet:Wallet {{address: sudo.seller}})
            return count(wallet)
            """
            count += self.query(create_wallets)[0].value()

        return count
    @count_query_logging
    def connect_sudo_power_users(self, context, urls):
        count = 0
        for url in urls: 
            query = f"""
            load csv with headers from '{url}' as sudo
            with collect(distinct(sudo.seller)) as addresses
            match (wallet:Wallet) 
            where wallet.address in addresses
            with wallet
            match (wallet)
            match (wic:_Wic:_{self.subgraph_name}:_{context})
            with wallet, wic
            merge (wallet)-[r:_HAS_CONTEXT]->(wic)
            return count(wallet)
            """
            logging.info(query)
            count += self.query(query)[0].value()

        return count 
        
        

