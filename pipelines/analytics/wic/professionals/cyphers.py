from .. import WICCypher
from ....helpers import count_query_logging
 
class ProfessionalsCyphers(WICCypher):
    def __init__(self, subgraph_name, conditions, database=None):
        WICCypher.__init__(self, subgraph_name, conditions, database)
 
 
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
    def identify_investors_bios(self, context):
        count = 0 
        label = """
        CALL db.index.fulltext.queryNodes("wicBios", "'investment fund' OR 'venture capital firm'  OR  'investing in' OR 'VC' OR 'investment firm' OR 'seed stage' OR 'pre-seed") 
        YIELD node 
        UNWIND node AS nn 
        SET nn:Investment"""
        self.query(label)

        connectDirect = f"""
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(twitter:Twitter:Investment) 
        WITH wallet
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(DISTINCT(wallet))
        """
        count += self.query(connectDirect)[0].value()

        connectIndirect = f"""
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(:Account)-[:BIO_MENTIONED]->(account:Account:Investor)
        WHERE NOT (wallet)-[:_HAS_CONTEXT]->(:_{context}:_{self.subgraph_name})
        WITH wallet
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (walelt)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(DISTINCT(wic))
        """
        count += self.query(connectIndirect)[0].value()
  
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

    @count_query_logging
    def get_dao_funding_recipients(self, context):
        count = 0
        snapshot = f"""
        MATCH (entity:Entity)-[:HAS_ACCOUNT]-(wallet:Wallet)-[trans:TRANSFERRED]->(otherWallet:Wallet)-[:_HAS_CONTEXT]-(wic:_Context)
        WHERE trans.nb_transfer > 1
        WITH otherWallet
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (otherWallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(DISTINCT(otherWallet))
        """
        count += self.query(snapshot)[0].value()

        propHouse = f"""
        MATCH (wallet:Wallet)-[:AUTHOR]->(proposal:Proposal:Winner)
        WITH wallet
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(DISTINCT(wallet))
        """
        count += self.query(propHouse)[0].value()

        return count 

    @count_query_logging
    def get_dao_treasury_funders(self, context):
        count = 0 
        query = f"""
        MATCH (entity:Entity)-[:HAS_ACCOUNT]-(wallet:Wallet)<-[trans:TRANSFERRED]-(otherWallet:Wallet)-[:_HAS_CONTEXT]-(wic:_Context)
        WHERE trans.nb_transfer > 1
        WITH otherWallet
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (otherWallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(DISTINCT(otherWallet))
        """
        count += self.query(query)[0].value()

        return count 

    @count_query_logging
    def get_technical_contributors(self, context):
        count = 0 
        query = f"""
        MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(g:Github)-[:CONTRIBUTOR]->(repo:Repository)-[:HAS_REPOSITORY]-(token:Token)-[:HAS_STRATEGY]-(:Entity)
        WITH wallet 
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(DISTINCT(wic))
        """
        count = self.query(query)[0].value()

        return count
