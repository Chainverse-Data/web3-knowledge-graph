from .. import WICCypher
from ....helpers import count_query_logging
import logging 

class ProfessionalsCyphers(WICCypher):
    def __init__(self, subgraph_name, conditions, database=None):
        WICCypher.__init__(self, subgraph_name, conditions, database)
        self.ensAdminLabel = "WicEnsAdmin"
        self.twitterInfluencerLabel = "WicTwitterInfluencer"
        self.founderLabel = "WicFounderLabel"
        self.podcasterLabel = "WicPodcasterLabel"
        self.investorLabel = "WicInvestorLabel"
        self.marketerLabel = "WicMarketerLabel"
        self.communityLeadLabel = "WicCommunityLead"
        self.devrelLabel = "WicDevrelLabel"
        self.companyOfficerLabel = "WicCompanyOfficer"

    @count_query_logging
    def label_eligible_accounts(self):
        query = """
        CALL apoc.periodic.commit("match (a:Account) where not a:Wallet and a.bio is not null 
        AND NOT a:HasBio 
        WITH a limit 10000 
        SET a:HasBio return count(*)")
        """
        self.query(query)



 #   @count_query_logging
 #   def create_bios_index(self):
#        query = f"""
 #       CREATE FULLTEXT INDEX {self.bios_index} FOR (n:Account) ON EACH [n.bio]
  #      """
   #     self.query(query)

    @count_query_logging
    def token_contract_deployer_wallets(self, context):
        query = f"""
        MATCH (deployer:Wallet)-[r:DEPLOYED]->(token:Token)
        WHERE ((token:Token)-[:HAS_STRATEGY]-(:Entity) OR (token)-[:HAS_ACCOUNT]-(:Twitter))
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
        MATCH (context:_Wic:_{self.subgraph_name}:_{context})
        MATCH (wallet)
        WITH wallet, context
        MATCH (wallet)
        MATCH (context)
        MERGE (wallet)-[r:_HAS_CONTEXT]->(context)
        RETURN COUNT(wallet)
        """
        count = self.query(query)[0].value()

        return count

    @count_query_logging
    def get_org_multisig_signers(self, context):
        query = f"""
        MATCH (multisig:MultiSig)<-[account:HAS_ACCOUNT]-(entity:Entity) 
        MATCH (wallet:Wallet)-[signer:IS_SIGNER]->(multisig)
        WITH wallet, multisig
        MATCH (wallet)
        MATCH (context:_Wic:_{self.subgraph_name}:_{context})
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
        remove = (f"""match (account:Account:{self.ensAdminLabel}) remove account:{self.ensAdminLabel}""")
        self.query(remove)

        labelsQuery = f"""
        MATCH (entity:Entity)-[:HAS_PROPOSAL]-(proposal:Proposal)
        MATCH (voter:Wallet)-[:VOTED]->(proposal)
        WITH entity, count(distinct(voter)) as voters
        where voters > 100
        MATCH (entity:Entity)-[:HAS_ALIAS]-(alias:Alias:Ens)
        SET entity:{self.ensAdminLabel}
        RETURN COUNT(entity)
        """
        self.query(labelsQuery)

        connectQuery =f"""
        MATCH (entity:Entity)-[:HAS_ALIAS]-(alias:Alias:Ens)
        MATCH (ensAdmin:Wallet:{self.ensAdminLabel})-[:HAS_ALIAS]-(alias:Alias)
        WITH ensAdmin
        MATCH (context:_Context:_{self.subgraph_name}:_{context})
        MERGE (ensAdmin)-[:_HAS_CONTEXT]->(context)
        RETURN COUNT(*)
        """
        count = self.query(connectQuery)

        remove = f"""match (ensAdmin:Wallet) remove ensAdmin:{self.ensAdminLabel}"""
        self.query(remove)

        clean = f"""
        MATCH (wallet:Wallet:{self.ensAdminLabel})
        REMOVE wallet:Wallet:{self.ensAdminLabel}
        """
        self.query(clean)

        return count


    @count_query_logging
    def identify_founders_bios(self, context):
        queryString = "'founder' OR 'co-founder'"

        remove = f"""
        match (nn:Account:{self.founderLabel})
        remove nn:{self.founderLabel}
        """
        self.query(remove)

        search = f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") YIELD node
        UNWIND node as founder
        WITH founder
        SET founder:{self.founderLabel}
        """
        logging.info(search)
        self.query(search)

        connect = f"""
        MATCH (account:Account:{self.founderLabel})
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH account, wic
        MERGE (account)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(*)"""
        count = self.query(connect)[0].value()

        clean = f"""
        match (nn:Account:{self.founderLabel})
        remove nn:{self.founderLabel}
        """
        self.query(clean)
        
        return count
            
        
    
    @count_query_logging
    def identify_podcasters_bios(self, context):
        queryString = "'podcasts' OR 'podcast'"

        remove = f"""match (account:Account:{self.podcasterLabel}) remove account:{self.podcasterLabel}"""
        self.query(remove)

        search =f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") YIELD node
        UNWIND node as podcaster
        WITH podcaster
        MATCH (podcaster:Account)
        WITH podcaster
        SET podcaster:{self.podcasterLabel}
        """
        self.query(search)

        connect = f"""
        match (account:Account:{self.podcasterLabel})
        match (context:_Wic:_{self.subgraph_name}:_{context})
        with account
        match (context:_Wic:_{self.subgraph_name}:_{context})
        merge (account)-[r:_HAS_CONTEXT]->(context)
        return count(distinct(account))
        """
        count = self.query(connect)[0].value()

        clean = f"""match (account:Account:{self.podcasterLabel}) remove account:{self.podcasterLabel}"""

        self.query(clean)
        self.query("""
        CALL apoc.periodic.commit("
        MATCH (account:Account)-[:_HAS_CONTEXT]-(context:_Context:_Professionals)
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(account)
        WHERE NOT (wallet)-[:_HAS_CONTEXT]->(context)
        WITH wallet, context 
        LIMIT 10000
        MATCH (wallet)
        MATCH (context)
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        RETURN COUNT(*)")
        """)
        return count


    @count_query_logging
    def identify_twitter_influencers_bios(self,context):
        
        self.query(f"""match (twitter:Twitter:{self.twitterInfluencerLabel}) remove twitter:{self.twitterInfluencerLabel}""")

        cutoff = f""""
        match (influencerTwitter:Twitter)
        WITH influencerTwitter
        MATCH (follow:Account)-[:HAS_ACCOUNT]-(follower:Twitter)-[:FOLLOWS]->(influencerTwitter)
        WITH influencerTwitter, count(distinct(follower)) as influencerFollowers
        RETURN apoc.agg.percentiles(influencerFollowers, [.75])[0]
        """
        logging.info(cutoff)
        cutoff = self.query(cutoff)[0].value()
        logging.info(cutoff)
        
        labelQuery = f"""
        MATCH (follow:Account)-[:HAS_ACCOUNT]-(follower:Twitter)-[:FOLLOWS]->(influencerTwitter)
        WITH influencerTwitter, count(distinct(follower)) as countFollowers
        WHERE countFollowers >= {cutoff}
        WITH influencerTwitter
        SET influencerTwitter:{self.twitterInfluencerLabel}
        RETURN COUNT(*)
        """
        logging.info(labelQuery)
        self.query(labelQuery)

        connect = f"""
        MATCH (influencerTwitter:Twitter:{self.twitterInfluencerLabel}
        MATCH (context:_Context:_{self.subgraph_name}:_{context})
        WITH context, influencerTwitter
        MERGE (influencerTwitter)-[r:_HAS_CONTEXT]->(context)
        RETURN COUNT(*)
        """
        logging.info(connect)
        count = self.query(connect)[0].value()

        clean = f"""
        match (twitter:Twitter:{self.twitterInfluencerLabel})
        remove twitter:{self.twitterInfluencerLabel}
        """
        logging.info(clean)
        self.query(clean)

        return count


    @count_query_logging
    def identify_investors_bios(self, context):
        remove = f"match (a:Account:{self.investorLabel}) remove a:{self.investorLabel}"
        self.query(remove)
        logging.info(remove)

        queryString = "'investor' OR 'investing' OR 'angel investor' OR 'GP' OR 'LP'"

        self.query(f"""match (account:Account:{self.investorLabel}) remove account:{self.investorLabel}""")
        label =f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") YIELD node, score
        UNWIND node as investor
        WITH investor
        MATCH (investor:HasBio:Account)
        SET investor:{self.investorLabel}"""
        self.query(label)
        logging.info(label)

        connect = f"""
        match (account:Account:{self.investorLabel})
        match (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        with account, wic
        merge (account)-[r:_HAS_CONTEXT]->(wic)
        RETURN COUNT(account)
        """
        count = self.query(connect)[0].value()

        clean = f"match (a:Account:{self.investorLabel}) remove a:{self.investorLabel}"
        logging.info(clean)

        return count


    @count_query_logging
    def identify_marketers_bios(self, context):
        queryString = """Marketing OR Marketer"""
        self.query(f"match (account:Account:{self.marketerLabel}) remove account:{self.marketerLabel}")

        label =f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") YIELD node, score
        UNWIND node as marketer
        WITH marketer
        MATCH (marketer:Account)
        SET marketer:{self.marketerLabel}"""
        self.query(label)

        connect = f"""
        match (account:Account:{self.marketerLabel})
        match (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        with account, wic
        merge (account)-[r:_HAS_CONTEXT]->(wic)
        RETURN COUNT(account)
        """
        count = self.query(connect)[0].value()
        clean = f"match (account:Account:{self.marketerLabel}) remove account:{self.marketerLabel}"
        self.query(clean)

        return count

    @count_query_logging
    def identify_community_lead_bios(self, context):
        queryString = """community lead OR community manager"""

        self.query(f"match (account:Account:{self.communityLeadLabel}) remove account:{self.communityLeadLabel}")

        label =f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") YIELD node, score
        UNWIND node as communityLead
        WITH communityLead
        MATCH (communityLead:Account)
        SET communityLead:{self.communityLeadLabel}"""
        self.query(label)

        connect = f"""
        match (account:Account:{self.communityLeadLabel})
        match (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        with account, wic
        merge (account)-[r:_HAS_CONTEXT]->(wic)
        RETURN COUNT(account)
        """
        count = self.query(connect)[0].value()
        clean = f"match (a:Account:{self.communityLeadLabel}) remove a:{self.communityLeadLabel}"
        self.query(clean)

        return count

    @count_query_logging
    def identify_devrel_bios(self, context):
        queryString = """'devrel' OR 'developer relations' OR 'ecosystem lead'"""

        remove = (f"match (a:Account:{self.devrelLabel}) remove a:{self.devrelLabel}")
        self.query(remove)

        label =f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") YIELD node, score
        UNWIND node as devRel
        WITH devRel
        MATCH (devRel:Account)
        SET devRel:{self.devrelLabel}"""
        self.query(label)

        connect = f"""
        match (account:Account:{self.devrelLabel})
        match (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        with account, wic
        merge (account)-[r:_HAS_CONTEXT]->(wic)
        RETURN COUNT(account)
        """
        count = self.query(connect)[0].value()
        clean = f"match (a:Account:{self.devrelLabel}) remove a:{self.devrelLabel}"
        self.query(clean)

        return count

    @count_query_logging
    def identify_company_officers_bios(self, context):
        queryString = "'VP' or 'Vice President' OR 'CEO' or 'Head of'"
        remove = f"match (a:Account:{self.companyOfficerLabel}) remove a:{self.companyOfficerLabel}"
        self.query(remove)

        label =f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") YIELD node
        UNWIND node as devrel
        WITH companyOfficer
        MATCH (companyOfficer:Account)
        SET companyOfficer:{self.companyOfficerLabel}"""
        self.query(label)

        connect = f"""
        MATCH (companyOfficer:Account:{self.companyOfficerLabel})
        MATCH (context:_Context:_{context}:_{self.subgraph_name}:{self.companyOfficerLabel})
        WITH context, companyOfficer
        merge (companyOfficer)-[r:_HAS_CONTEXT]->(context)
        return count(companyOfficer)"""
        count = self.query(connect)[0].value()

        self.query(f"match (a:Account:{self.companyOfficerLabel}) remove a:{self.companyOfficerLabel}")

        return count




    def connect_accounts_to_wallet_for_bios(self, context):
        connectWallets = f"""
        CALL apoc.periodic.commit("
        MATCH (account:Account)-[:_HAS_CONTEXT]->(context:_Context:_{self.subgraph_name}:_{context})
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(account)
        WHERE NOT (wallet)-[:_HAS_CONTEXT]->(context)
        WITH wallet, context 
        LIMIT 5000
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        RETURN COUNT(*)")
        """
        self.query(connectWallets)

        return None
        
        
