from .. import WICCypher
from ....helpers import Queries, count_query_logging
import logging 

class CreatorsCollectorsCypher(WICCypher):
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
            MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            MATCH (wallet:Wallet)-[:HAS_ALIAS]-(alias:Alias:Ens)
            WITH wallet, split(alias.name, ".eth")[0] AS ens_name, wic
            WHERE size(ens_name) = 3 
            WITH wallet, wic 
            MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
            RETURN count(con)
        """
        count = self.query(query)[0].value() 

        return count
    @count_query_logging
    def create_sudo_power_users(self, urls):
        count = 0
        for url in urls:
            create_wallets = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS sudo
                MERGE (wallet:Wallet {{address: sudo.seller}})
                RETURN count(wallet)
            """
            count += self.query(create_wallets)[0].value()

        return count

    @count_query_logging
    def connect_sudo_power_users(self, context, urls):
        count = 0
        for url in urls: 
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS sudo
                MATCH (wallet:Wallet {{address: sudo.seller}}) 
                MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
                WITH wallet, wic
                MERGE (wallet)-[r:_HAS_CONTEXT]->(wic)
                RETURN count(wallet)
            """
            logging.info(query)
            count += self.query(query)[0].value()

        return count 

    @count_query_logging
    def connect_blur_power_users(self, context, urls):
        count = 0
        for url in urls: 
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS blur
                MATCH (wallet:Wallet {{address: blur.address}}) 
                MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
                WITH wallet, wic
                MERGE (wallet)-[r:_HAS_CONTEXT]->(wic)
                RETURN count(wallet)
            """
            logging.info(query)
            count += self.query(query)[0].value()

        return count 

    @count_query_logging
    def connect_nft_borrowers(self, context, urls):
        count = 0 
        for url in urls:
            connect_wallets = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS borrower
                MATCH (wallet:Wallet {{address: borrower.address}})
                MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
                WITH wallet, wic
                MERGE (wallet)-[r:_HAS_CONTEXT]->(wic)
                RETURN count(wallet) 
            """
            logging.info(connect_wallets)
            count += self.query(connect_wallets)[0].value()
        
        return count 




    

        
        

