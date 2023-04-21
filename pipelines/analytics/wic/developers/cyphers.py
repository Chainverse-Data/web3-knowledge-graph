from .. import WICCypher
from ....helpers import count_query_logging

class DevelopersCyphers(WICCypher):
    def __init__(self, subgraph_name, conditions, database=None):
        WICCypher.__init__(self, subgraph_name, conditions, database)
        
    @count_query_logging
    def has_github(self, context):
        query = f"""
            WITH datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) AS timeNow
            MATCH (wallet:Wallet)-[r:HAS_ACCOUNT]-(github:Github:Account)
            MATCH (context:_Wic:_{self.subgraph_name}:_Context:_{context})
            WITH context, wallet, timeNow
            MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
            SET con.toRemove = null
            SET r.createdDt = timeNow 
            RETURN count(distinct(wallet)) AS count
        """
        count = self.query(query)[0].value()
        return count 

    @count_query_logging 
    def gitcoin_bounty_fulfill(self, context):
        query = f"""
            WITH datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) AS timeNow
            MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(user:Github:Account)-[:HAS_FULLFILLED]-(bounty:Gitcoin:Bounty)
            MATCH (context:_Wic:_{self.subgraph_name}:_Context:_{context})
            WITH wallet, context, timeNow, collect(distinct(bounty.uuid)) AS bountyUuids
            MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
            SET con.toRemove = null
            SET con.context = bountyUuids
            SET con.createdDt = timeNow
            RETURN count(distinct(wallet)) AS count
            """
        count = self.query(query)[0].value()
        return count
        
    @count_query_logging
    def gitcoin_bounty_admin(self, context):
        query = f"""
            WITH datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) AS datetime
            MATCH (w:Wallet)-[:HAS_ACCOUNT]-(user:Github:User)-[:IS_OWNER]-(bounty:Gitcoin:Bounty)
            MATCH (context:_Wic:_{self.subgraph_name}:_Context:_{context})
            WITH w, context, datetime, collect(distinct(bounty.uuid)) AS bountyUuids
            MERGE (w)-[con:_HAS_CONTEXT]->(context)
            SET con.toRemove = null
            SET con.createdDt = datetime
            SET con.context = bountyUuids
            RETURN count(distinct(w)) AS count
        """
        count = self.query(query)[0].value()
        return count 

    @count_query_logging
    def is_smart_contract_dev(self, context):
        query = f"""
            MATCH (repo:Github:Repository)
            WHERE (repo.description contains "smart contract" or repo.description contains "truffle" or repo.description contains "token contract" or repo.description contains ".sol" or repo.description contains "solidity")
            MATCH (repo)-[:CONTRIBUTOR|OWNER|SUBSCRIBER]-(:Github:User)-[:HAS_ACCOUNT]-(wallet:Wallet)
            OPTIONAL MATCH (wallet)-[:CONTRIBUTOR|OWNER|SUBSCRIBER]-(:Repository)-[:HAS_REPOSITORY]-(:Token)
            WITH wallet
            MATCH (context:_Wic:_{self.subgraph_name}:_Context:_{context})
            MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
            SET con.toRemove = null
            RETURN count(distinct(wallet))
        """
        count = self.query(query)[0].value()
        return count 

    @count_query_logging
    def identify_dune_accounts(self, context):
        query = f"""
        MATCH (wallet:Wallet)-[r:HAS_ACCOUNT]->(dune:Dune)
        MATCH (context:_Context:_Wic:_{self.subgraph_name}:_{context})
        WITH wallet, context
        MERGE (wallet)-[con:_HAS_CONTEXT]->(context)
        SET con.toRemove = null
        RETURN COUNT(DISTINCT(wallet))
        """
        count = self.query(query)[0].value()

        return count 

