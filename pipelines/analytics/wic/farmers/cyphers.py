from .. import WICCypher
from ....helpers import count_query_logging
import logging

class FarmerCyphers(WICCypher):
    def __init__(self, subgraph_name, conditions, database=None):
        WICCypher.__init__(self, subgraph_name, conditions, database)
        
    @count_query_logging
    def connect_suspicious_snapshot_daos(self, context):
        connect_wallets = f"""
            MATCH (context:_Wic:_{self.subgraph_name}:_Context:_{context})
            MATCH (wallet:Wallet)-[r:VOTED]->(p:Proposal)-[:HAS_PROPOSAL]-(entity:SuspiciousDao)
            WITH wallet, context
            MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
            SET con.toRemove = null
            RETURN count(distinct(wallet)) AS count
        """
        logging.info(connect_wallets)
        count = self.query(connect_wallets)[0].value()
        return count 

    @count_query_logging
    def remove_mirror_label(self):
        query = """
        MATCH (article:Article)
        WHERE article:MirrorFarmer
        REMOVE article:MirrorFarmer
        RETURN COUNT(*)
        """
        count = self.query(query)[0]

        return count 

    def get_mirror_benchmark(self):
        get_extreme = """
            MATCH (w:Wallet)-[r:AUTHOR]->(a:Article)
            WITH w, count(distinct(a)) AS articles
            WITH apoc.agg.percentiles(articles)[3] * 1.25 AS arts
            RETURN arts AS cutoff
        """
        cutoff = self.query(get_extreme)[0].value()

        return cutoff

    @count_query_logging
    def label_mirror(self, benchmark):
        query = f"""
        MATCH (wallet:Wallet)-[r:AUTHOR]->(article:Article)
        WITH wallet, COUNT(DISTINCT(article)) as articles
        WHERE articles >= {benchmark}
        SET wallet:MirrorFarmer
        RETURN COUNT(DISTINCT(wallet))
        """
        count = self.query(query)[0].value()
        
        return count

    @count_query_logging
    def connect_suspicious_mirror(self, context):
        connect_extreme = f"""
            MATCH (wallet:MirrorFarmer)
            MATCH (context:_Wic:_{self.subgraph_name}:_Context:_{context})
            WITH wallet, context
            MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
            SET con.toRemove = null
            RETURN count(distinct(wallet)) AS count
        """
        count = self.query(connect_extreme)[0].value()

        return count 

    @count_query_logging
    def clean(self):
        query = """
        MATCH (wallet:MirrorFarmer)
        REMOVE wallet:MirrorFarmer
        RETURN COUNT(DISTINCT(wallet))
        """
        count = self.query(query)[0].value()

        return count

    @count_query_logging
    def identify_nft_wash_traders(self, context, addresses):
        ## needs to be replaced lol
        ## this comes from an export of a dune dashboard
        query = f"""
        MATCH (wallet:Wallet) 
        WHERE wallet.address in $addresses
        WITH wallet
        MATCH (context:_Context:_Wic:_{context}:_{self.subgraph_name})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))
        """
        count = self.query(query, parameters={"addresses": addresses})[0].value()

        return count

    @count_query_logging
    def identify_spam_contract_deployers(self, context):
        query = f"""
        MATCH (wallet:Wallet)-[r:DEPLOYED]->(token:SpamContract)
        MATCH (wic:_Wic:_{context}:_{self.subgraph_name})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))
        """
        count = self.query(query)[0].value()

        return count 

    @count_query_logging
    def connect_cosigner_expansion(self, context):
        connect = f"""
            WITH datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) AS timeNow
            MATCH (wallet:Wallet)-[r:_HAS_CONTEXT]->(context:_Context:_{self.subgraph_name})
            MATCH (wallet:Wallet)-[:IS_SIGNER]-(:MultiSig)-[:IS_SIGNER]-(otherwallet)
            MATCH (cosigners:_Wic:_{self.subgraph_name}:_Context:_{context})
            WHERE NOT (otherwallet)-[:_HAS_CONTEXT]->(:_{self.subgraph_name})
            WITH otherwallet, cosigners, wallet, timeNow
            MATCH (otherwallet)
            MATCH (cosigners)
            MATCH (wallet)
            MERGE (otherwallet)-[con:_HAS_CONTEXT]->(cosigners)
            MERGE (otherwallet)-[conbud:_HAS_CONTEXT_BUDDY]->(wallet)
            SET con.toRemove = null
            SET conbud.toRemove = null
            SET conbud.`_context` = cosigners.`_displayName`
            SET con.createdDt = timeNow
            SET conbud.createdDt = timeNow
            RETURN count(otherwallet)
        """
        count = self.query(connect)[0].value()
        return count

    @count_query_logging
    def connect_farmer_counterparties(self, context):
        label = """
        MATCH (counterParty:Wallet)-[trans:TRANSFERRED]-(farmer:Wallet)-[:_HAS_CONTEXT]-(wic:_Farmers:_Context)
        WHERE NOT wic:_FarmerCosigner
        AND NOT counterParty:IgnoreForFarmers
        AND NOT (counterParty)-[:_HAS_CONTEXT]-(:_Farmers)
        AND trans.nb_transfer >= 3
        SET counterParty:FarmerCounterParty
        """
        self.query(label)

        connect = f"""
        MATCH (counterParty:FarmerCounterParty)
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH counterParty, wic
        MERGE (counterParty)-[con:_HAS_CONTEXT]->(wic)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(counterParty))
        """
        count = self.query(connect)[0].value()

        return count


        
        
