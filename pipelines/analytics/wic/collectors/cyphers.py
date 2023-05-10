from .. import WICCypher
from ....helpers import Queries, count_query_logging
import logging 

class CreatorsCollectorsCypher(WICCypher):
    def __init__(self, subgraph_name, conditions, database=None):
        WICCypher.__init__(self, subgraph_name, conditions, database)
        self.queries = Queries()

    @count_query_logging
    def cc_blue_chip(self, addresses, context):
        ## this makes sure our seed list is monitored across envs
        monitor = f"""
        MATCH (token:Token) where token.address in {addresses} set token.manualSelection = 'daily'
        """
        self.query(monitor)
        connect = f"""
            MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            MATCH (wallet:Wallet)-[r:HOLDS]->(token:Token)
            WHERE (token.address IN {addresses} OR token.contractAddress IN {addresses})
            WITH wallet, wic, count(distinct(token)) as count_collections
            WHERE count_collections > 1
            MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
            SET con.toRemove = null
            SET con.count = count_collections
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
            SET con.toRemove = null
            RETURN count(con)
        """
        count = self.query(query)[0].value() 

        return count
    
    @count_query_logging
    def get_mirror_collectors(self, context):
        query = f"""
        MATCH (author:Wallet)-[r:AUTHOR]->(a:Mirror)
        MATCH (author:Wallet)-[:_HAS_CONTEXT]->(wic:_Wic:_Context)
        WHERE NOT (author)-[:_HAS_CONTEXT]->(:_Farmers)
        WITH author, count(distinct(wic)) as wics
        WHERE wics >= 1
        WITH author
        MATCH (author)-[r:AUTHOR]->(article:Mirror)
        WITH author, count(distinct(article)) as arts
        WHERE arts >= 2
        MATCH (author)-[r:AUTHOR]->(article)-[:HAS_NFT]-(:ERC721)-[:HOLDS_TOKEN]-(collector:Wallet)
        WITH collector, count(distinct(article)) as arts
        WHERE arts >= 2
        MATCH (wic:_Wic:_Context:_{context}:_{self.subgraph_name})
        MERGE (collector)-[con:_HAS_CONTEXT]->(wic)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(collector))
        """
        count = self.query(query)[0].value()

        return count 

    @count_query_logging
    def get_web3_music_collectors(self, context):
        count = 0 
        ### gets collectors acc. neume
        neumeQuery = f"""
        MATCH  (wallet:Wallet)-[hol:HOLDS_TOKEN]->(music:Token:MusicNft)
        WITH wallet, count(distinct(hol)) as collected
        WHERE collected >= 1
        MATCH (context:_Context:_Wic:_{self.subgraph_name}:_{context})
        WITH wallet, context
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))
        """
        count += self.query(neumeQuery)[0].value()

        return count        
            