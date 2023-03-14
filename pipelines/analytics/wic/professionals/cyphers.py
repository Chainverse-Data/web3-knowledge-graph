from .. import WICCypher
from ....helpers import count_query_logging
import logging 
import pandas as pd

class ProfessionalsCyphers(WICCypher):
    def __init__(self, subgraph_name, conditions, database=None):
        WICCypher.__init__(self, subgraph_name, conditions, database)

    @count_query_logging
    def label_eligible_accounts(self):
        query = """
        CALL apoc.periodic.commit('match (a:Account) where not a:Wallet and a.bio is not null 
        AND NOT a:HasBio 
        WITH a limit 10000 
        SET a:HasBio return count(*)')
        """
        self.query(query)


    @count_query_logging
    def label_eligible_accounts(self):
        query = """
        CALL apoc.periodic.commit('match (a:Account) where not a:Wallet and a.bio is not null 
        AND NOT a:HasBio 
        WITH a limit 10000 
        SET a:HasBio return count(*)')
        """
        self.query(query)

    @count_query_logging
    def create_bios_index(self):
        query = f"""
        CREATE FULLTEXT INDEX {self.bios_index} FOR (n:Account) ON EACH [n.bio]
        """
        self.query(query)

    @count_query_logging
    def token_contract_deployer_wallets(self, context):
        query = f"""
        MATCH (deployer:Wallet)-[r:DEPLOYED]->(token:Token)
        WHERE ((token:Token)-[:HAS_STRATEGY]-(token) OR (token)-[:HAS_ACCOUNT]-(twitter:Twitter))
        WITH deployer
        MATCH (deployer)
        MATCH (wic:_Wic:_{self.subgraph_name}:_{context})
        WITH deployer, wic
        MERGE (deployer)-[r:_HAS_CONTEXT]->(wic)
        RETURN COUNT(DISTINCT(deployer))
        """
        count = self.query(query)[0].value()
        
        return count

    @count_query_logging
    def get_org_wallet_deployers(self, context):
        query = f"""
        MATCH (wallet:Wallet)-[deploy:DEPLOYED]->(org_wallet:Wallet)
        MATCH (entity:Entity)-[:HAS_ACCOUNT]-(org_wallet)
        WITH wallet, entity
        MATCH (context:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[r:_HAS_CONTEXT]->(context)
        RETURN COUNT(*)
        """
        count = self.query(query)[0].value()

        return count(count)

    @count_query_logging
    def get_org_multisig_signers(self, context):
        query = f"""
        MATCH (multisig:MultiSig)-[account:HAS_ACCOUNT]->(entity:Entity) 
        MATCH (wallet:Wallet)-[signer:IS_SIGNER]->(multisig)
        WITH wallet, multisig
        MATCH (wallet)
        MATCH (context:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, context
        MATCH (wallet)
        MATCH (context)
        MERGE (wallet)-[r:_HAS_CONTEXT]->(context)
        RETURN COUNT(*)
        """
        count = self.query(query)[0].value()

        return count

    def get_snapshot_contributors(self, context):
        query = f"""
        MATCH (entity:Entity)-[:HAS_ACCOUNT]-(wallet:Wallet)
        MATCH (walletother)-[:CONTRIBUTOR]->(entity)
        WITH entity, walletother
        MATCH (walletother)
        MATCH (context:_Context:_{self.subgraph_name}:_{context})
        MATCH (walletother)
        WITH context, walletother
        MERGE (walletother)-[r:_HAS_CONTEXT]->(context)
        RETURN COUNT(*)
        """
        count = self.query(query)[0].value()

        return count 

    def get_ens_admin(self, context):
        query = f"""
        MATCH (ensholder:Wallet)-[:HAS_ALIAS]->(alias:Alias:Ens)
        MATCH (entity:Entity)-[:HAS_ALIAS]-(alias)
        WITH ensholder
        MATCH (context:_Context:_{self.subgraph_name}:_{context})
        MATCH (ensholder)
        WITH context, ensholder
        MATCH (context)
        MATCH (ensholder)
        WITH context, ensholder
        MATCH (ensholder)-[con:_HAS_CONTEXT]->(context)
        RETURN COUNT(*)
        """
        count = self.query(query)[0].value()

        return count

    @count_query_logging
    def identify_founders_bios(self, context):
        queryString = "founder OR building"
        search = f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") YIELD node, score
        UNWIND node as founder
        WHERE (founder:HasBio:Account)
        WITH founder, score
        MATCH (founder)
        MATCH (wic:_Wic:_{self.subgraph_name}:_{context})
        WITH founder, wic, score
        MERGE (founder)-[r:_HAS_CONTEXT]->(wic)
        SET r.score = score
        RETURN COUNT(founder)
        """
        count = self.query(search)[0].value()
    
        return count

    @count_query_logging
    def identify_podcasters_bios(self, context):
        queryString = "podcasts OR podcast"
        search =f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") YIELD node, score
        UNWIND node as podcaster
        WHERE (podcaster:HasBio:Acount)
        WITH podcaster, score
        MATCH (founder)
        MATCH (wic:_Wic:_{self.subgraph_name}:_{context})
        WITH podcaster, wic, score
        MERGE (podcaster)-[r:_HAS_CONTEXT]->(wic)
        SET r.score = score
        RETURN COUNT(founder)"""
        count = self.query(search)[0].value()

        return count

    def identify_twitter_influencer_cutoff(self,context):
        query = """"
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(twitter:Twitter)
        WITH twitter
        MATCH (follow:Wallet)-[:HAS_ACCOUNT]-(follower:Twitter)-[:FOLLOWS]->(twitter)
        WHERE NOT id(follower) = id(twitter)
        WITH twitter, count(distinct(follow)) as influencerFollowers
        RETURN apoc.agg.percentiles(influencerFollowers, [.9])[0]
        """
        value = self.query(query)[0].value()
        
        return value

    def identify_twitter_influencers_bios(self, context, cutoff):
        query = f"""
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(account:Account:HasBio)
        WITH twitter
        MATCH (follow:Wallet)-[:HAS_ACCOUNT]-(follower:Twitter)-[:FOLLOWS]->(twitter)
        WHERE NOT id(follower) = id(twitter)
        WITH twitter, wallet, count(distinct(follow)) as influencerFollowers
        WHERE influencerFollowers >= {cutoff}
        MATCH (twitter)
        MATCH (wallet)
        MATCH (context:_Context:_{self.subgraph_name}:{context})
        MERGE (wallet)-[r:_HAS_CONTEXT]->(context)
        MERGE (twitter)-[r:_HAS_CONTEXT]->(twitter)
        RETURN COUNT(*)
        """
        count = self.query(query)[0].value()

        return count


    @count_query_logging
    def identify_investors_bios(self, context):
        queryString = "investors OR investing OR angel OR fund OR 'GP' or 'LP'"
        search =f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") YIELD node, score
        UNWIND node as investor
        WHERE (investor:HasBio:Account)
        WITH investor, score
        MATCH (investor)
        MATCH (wic:_Wic:_{self.subgraph_name}:_{context})
        WITH investor, wic, score
        MERGE (podcaster)-[r:_HAS_CONTEXT]->(wic)
        SET r.score = score
        RETURN COUNT(*)"""
        count = self.query(search)[0].value()

        return count 

    @count_query_logging
    def identify_company_officers_bios(self, context):
        queryString = "'VP' or 'Vice President' OR 'CEO' or 'Head of'"
        search =f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") YIELD node, score
        UNWIND node as officers
        WHERE (officer:HasBio:Account)
        WITH officer, score
        MATCH (investor)
        MATCH (wic:_Wic:_{self.subgraph_name}:_{context})
        WITH officer, wic, score
        MERGE (officer)-[r:_HAS_CONTEXT]->(wic)
        SET r.score = score
        RETURN COUNT(*)"""
        count = self.query(search)[0].value()

        return count 

    @count_query_logging
    def identify_marketers_bios(self, context):
        queryString = """Marketing OR Marketer"""
        search =f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") YIELD node, score
        UNWIND node as marketers
        WHERE (marketer:HasBio:Account)
        WITH marketer, score
        MATCH (marketer)
        MATCH (wic:_Wic:_{self.subgraph_name}:_{context})
        WITH marketer, wic, score
        MERGE (marketer)-[r:_HAS_CONTEXT]->(wic)
        SET r.score = score
        RETURN COUNT(*)"""
        count = self.query(search)[0].value()

        return count 

    @count_query_logging
    def identify_community_lead_bios(self, context):
        queryString = """community lead OR community manager"""
        search =f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") YIELD node, score
        UNWIND node as community
        WHERE (community:HasBio:Account)
        WITH community, score
        MATCH (community)
        MATCH (wic:_Wic:_{self.subgraph_name}:_{context})
        WITH community, wic, score
        MERGE (community)-[r:_HAS_CONTEXT]->(wic)
        SET r.score = score
        RETURN COUNT(*)"""
        count = self.query(search)[0].value()

        return count 

    @count_query_logging
    def identify_devrel_bios(self, context):
        queryString = """developer relations OR 'devrel' or 'ecosystem lead'"""
        search =f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") YIELD node, score
        UNWIND node as devrel
        WHERE (devrel:HasBio:Account)
        WITH community, score
        MATCH (devrel)
        MATCH (wic:_Wic:_{self.subgraph_name}:_{context})
        WITH devrel, wic, score
        MERGE (devrel)-[r:_HAS_CONTEXT]->(wic)
        SET r.score = score
        RETURN COUNT(*)"""
        count = self.query(search)[0].value()

        return count 


    @count_query_logging
    def connect_accounts_to_wallet_for_bios(self):
        query = f"""
        CALL apoc.periodic.commit("
        MATCH (twitter:HasBio)-[:_HAS_CONTEXT]-(context:_Context:_{self.subgraph_name})
        MATCH (wallet)-[:HAS_ACCOUNT]-(twitter)
        WHERE NOT (wallet)-[:_HAS_CONTEXT]->(context)
        WITH wallet, context 
        LIMIT 5000
        MATCH (wallet)
        MATCH (context)
        MERGE (wallet)-[:_HAS_CONTEXT]->(context)
        RETURN COUNT(*)
        """
        count = self.query(query)[0].value()

        return count
        
        
