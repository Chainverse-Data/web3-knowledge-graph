from .. import WICCypher
from ....helpers import count_query_logging
 
class InfluencersCyphers(WICCypher):
    def __init__(self, subgraph_name, conditions, database=None):
        WICCypher.__init__(self, subgraph_name, conditions, database)
 
    @count_query_logging
    def identify_podcasters_bios(self, context, queryString):
 
        label = f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") 
        YIELD node
        UNWIND node AS podcaster
        MATCH (podcaster)-[:HAS_ACCOUNT]-(wallet:Wallet)
        WHERE NOT wallet:PodcasterWallet
        SET wallet:PodcasterWallet"""
 
        self.query(label)
 
        connect = f"""
        MATCH (wallet:Wallet:PodcasterWallet)
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(DISTINCT(wallet))"""
 
        count = self.query(connect)[0].value()
 
        return count
 
    @count_query_logging
    def identify_twitter_influencers(self, context):
        query = f"""
        MATCH (influencerTwitter:Twitter)-[:HAS_ACCOUNT]-(influencerWallet:Wallet)
        WITH influencerTwitter, influencerWallet
        MATCH (followerWallet:Wallet)-[:HAS_ACCOUNT]-(follower:Twitter)-[:FOLLOWS]->(influencerTwitter)
        WITH influencerWallet, count(distinct(followerWallet)) as countFollowers
        WHERE countFollowers >= 75
        SET influencerWallet:InfluencerWallet"""
        self.query(query)
 
        connect = f"""
        MATCH (wallet:Wallet:InfluencerWallet)
        MATCH (wic:_Wic:_{context}:_{self.subgraph_name})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(DISTINCT(wallet))
        """
        count = self.query(connect)[0].value()
 
        return count