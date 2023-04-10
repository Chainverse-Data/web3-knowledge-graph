from .. import WICCypher
from ....helpers import count_query_logging
 
class ProfessionalsCyphers(WICCypher):
    def __init__(self, subgraph_name, conditions, database=None):
        WICCypher.__init__(self, subgraph_name, conditions, database)
 
    @count_query_logging
    def token_contract_deployer_wallets(self, context):
        query = f"""
        MATCH (deployer:Wallet)-[r:DEPLOYED]->(token:Token)
        MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
        MERGE (deployer)-[con:_HAS_CONTEXT]->(wic)
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
        RETURN COUNT(DISTINCT(wallet))
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
        RETURN COUNT(wallet)
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
        RETURN COUNT(walletother)
        """
        count = self.query(query)[0].value()
 
        return count 
 
    @count_query_logging
    def identify_founders_bios(self, context, queryString):
 
        label = f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") 
        YIELD node
        UNWIND node AS founder
        MATCH (founder)-[:HAS_ACCOUNT]-(wallet:Wallet)
        WHERE NOT wallet:FounderWallet
        SET wallet:FounderWallet"""
 
        self.query(label)
 
        connect = f"""
        MATCH (wallet:FounderWallet:Wallet)
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(DISTINCT(wallet))"""
 
        count = self.query(connect)[0].value()
 
        return count
 
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
    def identify_investors_bios(self, context, queryString):
 
        label = f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") 
        YIELD node
        UNWIND node AS investor
        MATCH (investor)-[:HAS_ACCOUNT]-(wallet:Wallet)
        WHERE NOT wallet:InvestorWallet
        SET wallet:InvestorWallet"""
 
        self.query(label)
 
        connect = f"""
        MATCH (wallet:Wallet:InvestorWallet)
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(DISTINCT(wallet))"""
 
        count = self.query(connect)[0].value()
 
        return count
 
    @count_query_logging
    def identify_marketers_bios(self, context, queryString):
 
        label = f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") 
        YIELD node
        UNWIND node AS marketer
        MATCH (marketer)-[:HAS_ACCOUNT]-(wallet:Wallet)
        WHERE NOT wallet:MarketerWallet
        SET wallet:MarketerWallet"""
 
        self.query(label)
 
        connect = f"""
        MATCH (wallet:Wallet:MarketerWallet)
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(DISTINCT(wallet))"""
        count = self.query(connect)[0].value()
 
        return count
 
    @count_query_logging
    def identify_community_lead_bios(self, context, queryString):
        label = f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") 
        YIELD node
        UNWIND node AS communityLead
        MATCH (communityLead)-[:HAS_ACCOUNT]-(wallet:Wallet)
        WHERE NOT wallet:CommunityLeadWallet
        SET wallet:CommunityLeadWallet"""
 
        self.query(label)
 
        connect = f"""
        MATCH (wallet:Wallet:CommunityLeadWallet)
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(DISTINCT(wallet))"""
        count = self.query(connect)[0].value()
 
        return count
 
 
    @count_query_logging
    def identify_devrel_bios(self, context, queryString):
        label = f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") 
        YIELD node
        UNWIND node AS devRel
        MATCH (devRel)-[:HAS_ACCOUNT]-(wallet:Wallet)
        WHERE NOT wallet:DevRelWallet
        SET wallet:DevRelWallet"""
 
        self.query(label)
 
        connect = f"""
        MATCH (wallet:Wallet:DevRelWallet)
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(DISTINCT(wallet))"""
        count = self.query(connect)[0].value()
 
        return count
 
    @count_query_logging
    def identify_company_officers_bios(self, context, queryString):
        label = f"""
        CALL db.index.fulltext.queryNodes("wicBios", "{queryString}") 
        YIELD node
        UNWIND node AS companyOfficer
        MATCH (companyOfficer)-[:HAS_ACCOUNT]-(wallet:Wallet)
        WHERE NOT wallet:CompanyOfficerWallet
        SET wallet:CompanyOfficerWallet"""
 
        self.query(label)
 
        connect = f"""
        MATCH (wallet:Wallet:CompanyOfficerWallet)
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(DISTINCT(wallet))"""
        count = self.query(connect)[0].value()
 
        return count