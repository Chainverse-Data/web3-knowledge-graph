from .. import WICCypher
from ....helpers import count_query_logging
 
class InfluencersCyphers(WICCypher):
    def __init__(self, subgraph_name, conditions, database=None):
        WICCypher.__init__(self, subgraph_name, conditions, database)


    @count_query_logging
    def get_mirror_influencer(self, context):
        mirrorThreshold = """
        MATCH (collector:Wallet)-[hold:HOLDS_TOKEN]->(:Token)-[:HAS_NFT]-(mirror:Mirror)
        MATCH (author:Wallet)-[aut:AUTHOR]->(mirror)
        WITH author, COUNT(DISTINCT(collector)) as collectors
        RETURN apoc.agg.percentiles(collectors)[3] * .9 as threshold"""
        threshold = self.query(mirrorThreshold)[0].value()
        
        mirrorConnect = f"""
        MATCH (author:Wallet)-[aut:AUTHOR]->(mirror)-[:HAS_NFT]-(:Token)-[:HOLDS_TOKEN]-(collector:Wallet)
        WITH author, count(distinct(collector)) as collectors
        WHERE collectors > {threshold}
        WITH author
        MATCH (wic:_Wic:_Context:_{context}:_{self.subgraph_name})
        MERGE (author)-[con:_HAS_CONTEXT]->(wic)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(author))"""
        count = self.query(mirrorConnect)[0].value()

        return count

    @count_query_logging
    def get_substack_influencer(self, context):
        count = 0 
        substackQuery = f"""
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(twitter:Twitter:Account)
        MATCH (wallet)-[:HAS_ACCOUNT]-(substack:Substack:Account)
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))
        """
        count += self.query(substackQuery)[0].value()

        twitterStuffs = f"""
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(twitter:Twitter:Account)
        WHERE (twitter.bio contains "substack" or twitter.name contains "substack" or twitter.handle contains "substack")
        WITH wallet
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wic))"""
        count += self.query(twitterStuffs)[0].value()

        newsy = f"""
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(twitter:Twitter:Account)
        WHERE (twitter.bio contains "newsletter" or twitter.name contains "newsletter" or twitter.handle contains "newsletter")
        WITH wallet
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wic))"""
        count += self.query(newsy)[0].value()

        return count 


    @count_query_logging
    def identify_podcasters(self, context):
        count = 0
        bioQuery = f"""
        CALL db.index.fulltext.queryNodes("wicBios", "'podcaster' OR 'podcast'") 
        YIELD node
        UNWIND node AS podcaster
        MATCH (podcaster)-[:HAS_ACCOUNT]-(wallet:Wallet)
        WITH wallet
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wic))"""
        count += self.query(bioQuery)[0].value()
 
        otherAspects = f"""
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(twitter:Twitter)
        WHERE (twitter.name contains "podcast" or twitter.handle contains "podcast")
        WITH wallet
        MATCH (wic:_Wic:_Context:_{context}:_{self.subgraph_name})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))
        """
        count += self.query(otherAspects)[0].value()

        websites = f"""
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(website:Website:Account)
        WHERE (website.url contains "podcast" OR website.url contains "podcasts")
        WITH wallet
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))
        """
        count += self.query(websites)[0].value()
 
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
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))
        """
        count = self.query(connect)[0].value()
 
        return count

    @count_query_logging
    def get_dune_influencers(self, context):
        threshold = """
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(dune:Dune:Account)
        WITH wallet, dune.follows as follows
        RETURN apoc.agg.percentiles(follows)[3] * .9
        """
        cutoff = self.query(threshold)[0].value()

        connect = f"""
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(dune:Dune:Account)
        WHERE dune.follows > {cutoff}
        WITH wallet
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))
        """
        count = self.query(connect)[0].value()

        return count 