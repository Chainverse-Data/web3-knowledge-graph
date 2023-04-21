
from .. import WICCypher
from ....helpers import Queries, count_query_logging

class DaoCyphers(WICCypher):
    def __init__(self, subgraph_name, conditions, database=None):
        WICCypher.__init__(self, subgraph_name, conditions, database)
        self.queries = Queries()


    @count_query_logging
    def get_org_multisig_signers(self, context):
        query = f"""
        MATCH (multisig:MultiSig)<-[account:HAS_ACCOUNT]-(entity:Entity) 
        MATCH (wallet:Wallet)-[signer:IS_SIGNER]->(multisig)
        MATCH (context:_Wic:_{self.subgraph_name}:_Context:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
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
        MERGE (walletother)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(walletother)
        """
        count = self.query(query)[0].value()
        return count 
 
    @count_query_logging
    def get_dao_funding_recipients(self, context):
        count = 0
        snapshot = f"""
        MATCH (entity:Entity)-[:HAS_ACCOUNT]-(wallet:Wallet)-[trans:TRANSFERRED]->(otherWallet:Wallet)-[:_HAS_CONTEXT]-(wic:_Context)
        WHERE trans.nb_transfer >= 2
        WITH otherWallet
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (otherWallet)-[con:_HAS_CONTEXT]->(wic)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(otherWallet))
        """
        count += self.query(snapshot)[0].value()

        propHouse = f"""
        MATCH (wallet:Wallet)-[:AUTHOR]->(proposal:Proposal:Winner)
        WITH wallet
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))
        """
        count += self.query(propHouse)[0].value()
        return count 

    ## can update this once we have Twitter / Token
    @count_query_logging
    def get_dao_treasury_funders(self, context):
        count = 0 
        query = f"""
        MATCH (entity:Entity)-[:HAS_ACCOUNT]-(wallet:Wallet)<-[trans:TRANSFERRED]-(otherWallet:Wallet)-[:_HAS_CONTEXT]-(wic:_Context)
        WHERE trans.nb_transfer >= 2
        WITH otherWallet
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        MERGE (otherWallet)-[con:_HAS_CONTEXT]->(wic)
        SET con.toRemove = null
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
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))
        """
        count = self.query(query)[0].value()
        return count

