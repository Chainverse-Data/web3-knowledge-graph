from .. import WICCypher
from ....helpers import count_query_logging
import logging 

class ProfessionalsCyphers(WICCypher):
    def __init__(self, subgraph_name, conditions, database=None):
        WICCypher.__init__(self, subgraph_name, conditions, database)

    @count_query_logging
    def token_contract_deployer_wallets(self, context):
        query = f"""
        MATCH (deployer:Wallet)-[r:DEPLOYED]->(token:Token)
        MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
        MERGE (deployer)-[r:_HAS_CONTEXT]->(wic)
        RETURN COUNT(r)
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging
    def get_org_wallet_deployers(self, context):
        query = f"""
        MATCH (wallet:Wallet)-[deploy:DEPLOYED]->(org_wallet:Wallet)-[:HAS_ACCOUNT]-(entity:Entity)
        MATCH (context:_Wic:_{self.subgraph_name}:_Context:_{context})
        MERGE (wallet)-[r:_HAS_CONTEXT]->(context)
        RETURN COUNT(r)
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging
    def get_org_multisig_signers(self, context):
        query = f"""
        MATCH (multisig:MultiSig)<-[account:HAS_ACCOUNT]-(entity:Entity) 
        MATCH (wallet:Wallet)-[signer:IS_SIGNER]->(multisig)
        MATCH (context:_Wic:_{self.subgraph_name}:_Context:_{context})
        MERGE (wallet)-[r:_HAS_CONTEXT]->(context)
        RETURN COUNT(r)
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging
    def get_snapshot_contributors(self, context):
        query = f"""
        MATCH (entity:Entity)-[:HAS_ACCOUNT]-(wallet:Wallet)
        MATCH (walletother)-[:CONTRIBUTOR]->(entity)
        MATCH (context:_Wic:_{self.subgraph_name}:_Context:_{context})
        MERGE (walletother)-[r:_HAS_CONTEXT]->(context)
        RETURN COUNT(r)
        """
        count = self.query(query)[0].value()

        return count 

    @count_query_logging
    def get_ens_admin(self, context):
        query = f"""
        MATCH (entity:Entity)-[:HAS_PROPOSAL]-(proposal:Proposal)<-[:VOTED]-(voter:Wallet)
        WITH entity, count(distinct(voter)) as voters
        WHERE voters > 100 AND (entity)-[:HAS_ALIAS]-(Alias:Ens)
        MATCH (entity)-[:HAS_ALIAS]-(alias:Alias:Ens)-[:HAS_ALIAS]-(ensAdmin:Wallet)
        MATCH (context:_Wic:_{self.subgraph_name}:_Context:_{context})
        MERGE (ensAdmin)-[r:_HAS_CONTEXT]->(context)
        RETURN COUNT(r)
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging
    def identify_founders_bios(self, context, queryString):
        query = f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") 
        YIELD node
        UNWIND node AS founder
        MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
        MERGE (founder)-[con:_HAS_CONTEXT]->(wic)
        RETURN count(con)
        """
        count = self.query(query)[0].value()
        return count
    
    @count_query_logging
    def identify_podcasters_bios(self, context, queryString):
        query =f"""
            CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") YIELD node
            UNWIND node as podcaster
            WITH podcaster
            MATCH (context:_Wic:_{self.subgraph_name}:_Context:_{context})
            MERGE (account)-[r:_HAS_CONTEXT]->(context)
            RETURN count(distinct(account))
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging
    def identify_twitter_influencers_bios(self, context):
        query = f"""
            MATCH (influencerTwitter:Twitter)
            WITH influencerTwitter
            MATCH (follower:Twitter)-[:FOLLOWS]->(influencerTwitter)
            WITH influencerTwitter, count(distinct(follower)) as countFollowers
            WITH apoc.agg.percentiles(countFollowers, [.75])[0] as cutoff, countFollowers, influencerTwitter
            WHERE countFollowers >= cutoff
            MATCH (context:_Wic:_{self.subgraph_name}:_Context:_{context})
            MERGE (influencerTwitter)-[r:_HAS_CONTEXT]->(context)
            RETURN COUNT(r)
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging
    def identify_investors_bios(self, context, queryString):
        query = f"""
            CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") YIELD node, score
            UNWIND node as investor
            WITH investor
            MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            MERGE (investor)-[r:_HAS_CONTEXT]->(wic)
            RETURN COUNT(r)
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging
    def identify_marketers_bios(self, context, queryString):
        query =f"""
            CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") YIELD node, score
            UNWIND node as marketer
            WITH marketer
            MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            MERGE (marketer)-[r:_HAS_CONTEXT]->(wic)
            RETURN COUNT(r)
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging
    def identify_community_lead_bios(self, context, queryString):
        query =f"""
            CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") YIELD node, score
            UNWIND node as communityLead
            WITH communityLead
            MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            MERGE (communityLead)-[r:_HAS_CONTEXT]->(wic)
            RETURN COUNT(r)
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging
    def identify_devrel_bios(self, context, queryString):
        query =f"""
            CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") YIELD node, score
            UNWIND node as devRel
            WITH devRel
            match (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            merge (devRel)-[r:_HAS_CONTEXT]->(wic)
            RETURN COUNT(r)
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging
    def identify_company_officers_bios(self, context, queryString):
        query =f"""
            CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") YIELD node
            UNWIND node as companyOfficer
            WITH companyOfficer
            MATCH (context:_Wic:_{self.subgraph_name}:_Context:_{context})
            MERGE (companyOfficer)-[r:_HAS_CONTEXT]->(context)
            RETURN COUNT(r)
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging
    def connect_account_contexts_to_wallets(self):
        connectWallets = f"""
            CALL apoc.periodic.commit("
                MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(account:Account)-[:_HAS_CONTEXT]->(context:_Wic:_{self.subgraph_name}:_Context)
                WHERE NOT (wallet)-[:_HAS_CONTEXT]->(context)
                LIMIT 10000
                MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
                RETURN COUNT(*)
            ")
        """
        count = self.query(connectWallets)[0].value()
        return count
        
        
