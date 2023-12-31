from .. import WICCypher
from ....helpers import count_query_logging
import logging

class InterestsCyphers(WICCypher):
    def __init__(self, subgraph_name, conditions, database=None):
        WICCypher.__init__(self, subgraph_name, conditions, database)
        
    @count_query_logging
    def find_music_interested(self, context):
        count = 0
        collectorsQuery = f"""
        MATCH (wallet:Wallet)-[holds:HOLDS_TOKEN]->(token:Token:ERC721:MusicNft)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))
        """
        count += self.query(collectorsQuery)[0].value()

        accountsQuery = f"""
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]->(sound:Sound:Account)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))
        """
        count += self.query(accountsQuery)[0].value()

        biosQuery = f"""        
        CALL db.index.fulltext.queryNodes("wicBios", "'music' OR 'album' OR 'musician'")
        YIELD node
        UNWIND node as music 
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(music)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(biosQuery)[0].value()

        articlesQuery = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'music' OR 'musician' OR 'concert'")
        YIELD node
        UNWIND node as music 
        MATCH (wallet:Wallet)-[:AUTHOR]->(music:Article:Mirror)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesQuery)[0].value()

        articlesCollectors = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'music' OR 'musician'")
        YIELD node
        UNWIND node as music 
        MATCH (music:Article:Mirror)-[:HAS_NFT]-(:ERC721)-[:HOLDS_TOKEN|HOLDS]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesCollectors)[0].value()
        
        twitterMentioned = f"""
        MATCH (wic:_Wic:_Context:_{context}:_{self.subgraph_name})
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(twitter:Twitter)
        MATCH (otherWallet)-[:HAS_ACCOUNT]-(:Twitter)-[:BIO_MENTIONED]-(twitter)
        WITH otherWallet, wic
        MERGE (otherWallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(otherWallet))
        """
        count += self.query(twitterMentioned)[0].value()
        return count 

    @count_query_logging
    def find_gaming_interested(self, context):
        count = 0
        biosQuery = f"""        
        CALL db.index.fulltext.queryNodes("wicBios", "'gaming' OR 'video games' or 'gamer'")
        YIELD node
        UNWIND node as gaming 
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(gaming)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(biosQuery)[0].value()

        articlesQuery = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'gaming' OR 'video games' or 'gamer'")
        YIELD node
        UNWIND node as gamer 
        MATCH (wallet:Wallet)-[:AUTHOR]->(gamer:Article:Mirror)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesQuery)[0].value()

        articlesCollectors = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'gaming' OR 'video games' or 'gamer'")
        YIELD node
        UNWIND node as gaming 
        MATCH (gaming:Article:Mirror)-[:HAS_NFT]-(:ERC721)-[:HOLDS_TOKEN|HOLDS]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesCollectors)[0].value()

        grants = f"""        
        CALL db.index.fulltext.queryNodes("grantTitle", "'gaming' OR 'video games' or 'gamer'")
        YIELD node
        UNWIND node as gaming 
        MATCH (gaming:Grant)-[]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(grants)[0].value()

        return count 

    @count_query_logging
    def find_outdoors_interested(self, context):
        count = 0
        biosQuery = f"""        
        CALL db.index.fulltext.queryNodes("wicBios", "'outdoors' OR 'nature'")
        YIELD node
        UNWIND node as outdoors 
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(outdoors)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(biosQuery)[0].value()

        articlesQuery = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'outdoors' OR 'nature'")
        YIELD node
        UNWIND node as outdoors 
        MATCH (wallet:Wallet)-[:AUTHOR]->(outdoors:Article)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesQuery)[0].value()

        articlesCollectors = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'outdoors' OR 'nature'")
        YIELD node
        UNWIND node as outdoors 
        MATCH (outdoors:Article)-[:HAS_NFT]-(:ERC721)-[:HOLDS_TOKEN|HOLDS]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesCollectors)[0].value()


        grants = f"""        
        CALL db.index.fulltext.queryNodes("grantTitle", "'outdoors' OR 'nature'")
        YIELD node
        UNWIND node as outdoors 
        MATCH (outdoors:Grant)-[]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(grants)[0].value()

        return count 

    @count_query_logging
    def find_film_video(self, context):
        count = 0
        biosQuery = f"""        
        CALL db.index.fulltext.queryNodes("wicBios", "'movies' OR 'cinema'")
        YIELD node
        UNWIND node as film_video 
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(film_video)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(biosQuery)[0].value()

        articlesQuery = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'movies' OR 'cinema'")
        YIELD node
        UNWIND node as film_video 
        MATCH (wallet:Wallet)-[:AUTHOR]->(film_video:Article:Mirror)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesQuery)[0].value()

        articlesCollectors = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'movies' OR 'cinema'")
        YIELD node
        UNWIND node as film 
        MATCH (film:Article:Mirror)-[:HAS_NFT]-(:ERC721)-[:HOLDS_TOKEN|HOLDS]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesCollectors)[0].value()

        grants = f"""        
        CALL db.index.fulltext.queryNodes("grantTitle", "'movies' OR 'cinema'")
        YIELD node
        UNWIND node as film 
        MATCH (film:Grant)-[]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(grants)[0].value()

        return count

    @count_query_logging
    def find_photography(self, context):
        count = 0
        biosQuery = f"""        
        CALL db.index.fulltext.queryNodes("wicBios", "'photography' OR 'photographer'")
        YIELD node
        UNWIND node as photo 
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(photo)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(biosQuery)[0].value()

        articlesQuery = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'photography' OR 'photographer'")
        YIELD node
        UNWIND node as photo 
        MATCH (wallet:Wallet)-[:AUTHOR]->(photo:Article:Mirror)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesQuery)[0].value()

        articlesCollectors = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'photography' OR 'photographer'")
        YIELD node
        UNWIND node as photo 
        MATCH (photo:Article:Mirror)-[:HAS_NFT]-(:ERC721)-[:HOLDS_TOKEN|HOLDS]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesCollectors)[0].value()

        grants = f"""        
        CALL db.index.fulltext.queryNodes("grantTitle", "'photography' OR 'photographer'")
        YIELD node
        UNWIND node as photo 
        MATCH (photo:Grant)-[]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(grants)[0].value()

        return count

    @count_query_logging
    def find_culture(self, context):
        count = 0
        biosQuery = f"""        
        CALL db.index.fulltext.queryNodes("wicBios", "'cultural commentary' OR 'web3 culture'")
        YIELD node
        UNWIND node as culture 
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(culture)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(biosQuery)[0].value()

        articlesQuery = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'cultural commentary' OR 'web3 culture'")
        YIELD node
        UNWIND node as culture 
        MATCH (wallet:Wallet)-[:AUTHOR]->(culture:Article:Mirror)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesQuery)[0].value()

        articlesCollectors = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'cultural commentary' OR 'web3 culture'")
        YIELD node
        UNWIND node as culture 
        MATCH (culture:Article:Mirror)-[:HAS_NFT]-(:ERC721)-[:HOLDS_TOKEN|HOLDS]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesCollectors)[0].value()

        grants = f"""        
        CALL db.index.fulltext.queryNodes("grantTitle", "'cultural commentary' OR 'web3 culture'
        OR 'boys club'")
        YIELD node
        UNWIND node as culture 
        MATCH (culture:Grant)-[]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(grants)[0].value()

        return count

    @count_query_logging
    def find_writing_publishing(self, context):
        count = 0
        mirrorAuthor = f"""
        MATCH (wallet:Wallet)-[:AUTHOR]->(mirror:Article)
        WITH wallet, count(distinct(mirror)) as cn
        WHERE cn > 3
        AND cn < 3 
        WITH wallet
        MATCH (context:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))
        """
        count += self.query(mirrorAuthor)[0].value()

        mirrorCollector = f"""
        MATCH (article:Mirror)-[:HAS_NFT]-(token:ERC721)-[:HOLDS_TOKEN]-(wallet:Wallet)
        WITH wallet, count(distinct(article)) as arts
        WHERE arts > 1
        MATCH (context:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(context))
        """
        count += self.query(mirrorCollector)[0].value()

        biosQuery = f"""        
        CALL db.index.fulltext.queryNodes("wicBios", "'writing at' OR 'substack' OR 'author' OR 'newsletter'")
        YIELD node
        UNWIND node as writer 
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(writer)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (writer)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(writer))"""
        count += self.query(biosQuery)[0].value()

        grants = f"""        
        CALL db.index.fulltext.queryNodes("grantTitle", "'writing at' OR 'substack' OR 'author' OR 'newsletter'")
        YIELD node
        UNWIND node as writing 
        MATCH (culture:Grant)-[]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(grants)[0].value()

        return count

    @count_query_logging
    def find_data_scientists(self, context):
        count = 0
        biosQuery = f"""        
        CALL db.index.fulltext.queryNodes("wicBios", "'data science' OR 'data scientist' OR 'machine learning engineer'")
        YIELD node
        UNWIND node as data 
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(data)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(biosQuery)[0].value()

        articlesQuery = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'data science' OR 'data scientist' OR 'machine learning engineer'")
        YIELD node
        UNWIND node as datascience 
        MATCH (wallet:Wallet)-[:AUTHOR]->(datascience:Article:Mirror)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesQuery)[0].value()

        articlesCollectors = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'data science' OR 'data scientist' OR 'machine learning engineer'")
        YIELD node
        UNWIND node as datascience 
        MATCH (datascience:Article:Mirror)-[:HAS_NFT]-(:ERC721)-[:HOLDS_TOKEN|HOLDS]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesCollectors)[0].value()

        grants = f"""        
        CALL db.index.fulltext.queryNodes("grantTitle", "'data science' OR 'data scientist' OR 'machine learning engineer'")
        YIELD node
        UNWIND node as datascience 
        MATCH (datascience:Grant)-[]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(grants)[0].value()

        return count

    @count_query_logging
    def find_desci(self, context):
        count = 0
        biosQuery = f"""        
        CALL db.index.fulltext.queryNodes("wicBios", "'desci' OR 'decentralized science'")
        YIELD node
        UNWIND node as desci 
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(desci)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(biosQuery)[0].value()

        articlesQuery = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'desci' OR 'decentralized science'")
        YIELD node
        UNWIND node as desci 
        MATCH (wallet:Wallet)-[:AUTHOR]->(desci:Article:Mirror)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesQuery)[0].value()

        articlesCollectors = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'desci' OR 'decentralized science'")
        YIELD node
        UNWIND node as desci 
        MATCH (desci:Article:Mirror)-[:HAS_NFT]-(:ERC721)-[:HOLDS_TOKEN|HOLDS]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesCollectors)[0].value()

        grants = f"""        
        CALL db.index.fulltext.queryNodes("grantTitle", "'desci' OR 'decentralized science'")
        YIELD node
        UNWIND node as desci 
        MATCH (desci:Grant)-[]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(grants)[0].value()

        return count

    @count_query_logging
    def find_dei(self, context):
        count = 0
        biosQuery = f"""        
        CALL db.index.fulltext.queryNodes("wicBios", "'diversity equity and inclusion' OR 'dei'")
        YIELD node
        UNWIND node as dei 
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(dei)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(biosQuery)[0].value()

        articlesQuery = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'diversity equity and inclusion' OR 'dei'")
        YIELD node
        UNWIND node as dei 
        MATCH (wallet:Wallet)-[:AUTHOR]->(dei:Article:Mirror)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesQuery)[0].value()

        articlesCollectors = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'diversity equity and inclusion' OR 'dei'")
        YIELD node
        UNWIND node as dei 
        MATCH (dei:Article:Mirror)-[:HAS_NFT]-(:ERC721)-[:HOLDS_TOKEN|HOLDS]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesCollectors)[0].value()

        grants = f"""        
        CALL db.index.fulltext.queryNodes("grantTitle", "'diversity equity and inclusion' OR 'dei'")
        YIELD node
        UNWIND node as dei 
        MATCH (dei:Grant)-[]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(grants)[0].value()

        return count

    @count_query_logging
    def find_regen(self, context):
        count = 0 
        biosQuery = f"""        
        CALL db.index.fulltext.queryNodes("wicBios", "'regen' OR 'refi'")
        YIELD node
        UNWIND node as refi 
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(refi)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(biosQuery)[0].value()

        articlesQuery = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'regen' OR 'refi'")
        YIELD node
        UNWIND node as refi 
        MATCH (wallet:Wallet)-[:AUTHOR]->(refi:Article:Mirror)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesQuery)[0].value()

        articlesCollectors = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'regen' OR 'refi'")
        YIELD node
        UNWIND node as refi 
        MATCH (refi:Article:Mirror)-[:HAS_NFT]-(:ERC721)-[:HOLDS_TOKEN|HOLDS]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesCollectors)[0].value()

        grants = f"""        
        CALL db.index.fulltext.queryNodes("wicGrants", "'regen' OR 'refi'")
        YIELD node
        UNWIND node as refi 
        MATCH (refi:Grant)-[]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(grants)[0].value()

        return count

    @count_query_logging
    def find_ed(self, context):
        count = 0 
        biosQuery = f"""        
        CALL db.index.fulltext.queryNodes("wicBios", "'education' OR 'educator' OR 'teacher'")
        YIELD node
        UNWIND node as edu 
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(edu)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(biosQuery)[0].value()

        articlesQuery = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'education' OR 'educator' OR 'teacher'")
        YIELD node
        UNWIND node as edu 
        MATCH (wallet:Wallet)-[:AUTHOR]->(edu:Article:Mirror)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesQuery)[0].value()

        articlesCollectors = f"""        
        CALL db.index.fulltext.queryNodes("articleTitle", "'education' OR 'educator' OR 'teacher'")
        YIELD node
        UNWIND node as edu 
        MATCH (edu:Article:Mirror)-[:HAS_NFT]-(:ERC721)-[:HOLDS_TOKEN|HOLDS]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(articlesCollectors)[0].value()

        grants = f"""        
        CALL db.index.fulltext.queryNodes("grantTitle", "'education' OR 'educator' OR 'teacher'")
        YIELD node
        UNWIND node as edu 
        MATCH (edu:Grant)-[]-(wallet:Wallet)
        WITH wallet
        MATCH (context:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(grants)[0].value()

        return count
