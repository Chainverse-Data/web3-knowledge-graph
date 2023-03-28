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
            MERGE (wallet)-[r:_HAS_CONTEXT]->(context)
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
            MERGE (wallet)-[r:_HAS_CONTEXT]->(context)
            SET r.context = bountyUuids
            SET r.createdDt = timeNow
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
            MERGE (w)-[r:_HAS_CONTEXT]->(context)
            SET r.createdDt = datetime
            SET r.context = bountyUuids
            RETURN count(distinct(w)) AS count
        """
        count = self.query(query)[0].value()
        return count 

    ##def smart_contract_deployers(self):

    ## smart contract deployers

    ## authors of technical proposals, articles, and grants

    ##
