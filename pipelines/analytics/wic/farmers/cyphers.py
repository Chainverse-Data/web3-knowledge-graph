from .. import WICCypher
from ....helpers import count_query_logging

class FarmerCyphers(WICCypher):
    def __init__(self, subgraph_name, conditions, database=None):
        WICCypher.__init__(self, subgraph_name, conditions, database)
        
    @count_query_logging
    def connect_suspicious_snapshot_daos(self, context):
        connect_wallets = f"""
            WITH datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) AS timeNow
            MATCH (context:_Wic:_{self.subgraph_name}:_Context:_{context})
            MATCH (wallet:Wallet)-[r:VOTED]->(p:Proposal)-[:HAS_PROPOSAL]-(entity:Entity)-[:HAS_STRATEGY]-(token:Token)
            WHERE token.symbol in ['DAI', 'USDC', 'ETH', 'WETH', 'USDT']
            WITH wallet, context, timeNow, entity.snapshotId AS snapshotId
            MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
            SET con.createdDt = timeNow
            SET con._context = "This wallet participated in a DAO that does not use its native token to establish voting power: " +  snapshotId + " ."
            RETURN count(distinct(wallet)) AS count
        """
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
            WITH apoc.agg.percentiles(articles)[4] AS arts
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
    def identify_nft_wash_traders(self, context):
        ## needs to be replaced lol
        ## this comes from an export of a dune dashboard
        query = f"""
        MATCH (wallet:ActualWashTrader) // comes
        MATCH (context:_Context:_Wic:_{context}:_{self.subgraph_name})
        WITH wallet, context
        MATCH (wallet)-[con:_CONTEXT]->(context)
        RETURN COUNT(DISTINCT(wallet))
        """
        count = self.query(query)[0].value()

        return count

    @count_query_logging
    def identify_spam_contract_deployers(self, context):
        query = f"""
        MATCH (wallet:Wallet)-[r:DEPLOYED]->(token:SpamContract)
        MATCH (wic:_Wic:_{context}:_{self.subgraph_name})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
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
            SET conbud.`_context` = cosigners.`_displayName`
            SET con.createdDt = timeNow
            SET conbud.createdDt = timeNow
            RETURN count(otherwallet)
        """
        count = self.query(connect)[0].value()
        return count


        
        
