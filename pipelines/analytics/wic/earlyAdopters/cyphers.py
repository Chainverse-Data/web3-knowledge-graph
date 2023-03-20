

from .. import WICCypher
from ....helpers import count_query_logging

class EarlyAdoptersCyphers(WICCypher):
    def __init__(self, subgraph_name, conditions, database=None):
        super().__init__(subgraph_name, conditions, database)

    @count_query_logging
    def connect_early_daohaus_users(self, context):
        query = f"""
            MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            MATCH (wallet:Wallet)-[vote:VOTED]->(:Proposal:DaoHaus)
            WHERE vote.votedDt < datetime('2020-12-31')
            WITH wallet, wic, vote.votedDt as votedDt
            MERGE (wallet)-[edge:HAS_CONTEXT]->(wic)
            SET edge._context = votedDt
            RETURN count(distinct(wallet)) 
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging    
    def connect_early_snapshot_users(self, context):
        query = f"""
            MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            MATCH (proposal:Proposal:Snapshot)
            WHERE proposal.startDt < datetime("2020-12-31")
            WITH wic, proposal
            MATCH (wic)
            MATCH (wallet:Wallet)-[vote:VOTED]->(proposal)
            WITH wallet, wic, vote.votedDt AS votedDt
            MERGE (wallet)-[edge:_HAS_CONTEXT]->(wic)
            SET edge._context = votedDt
            RETURN count(distinct(wallet)) 
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging    
    def connect_dao_summoners(self, context):
        query = f"""
            MATCH (wallet:Wallet)-[r:SUMMONER]->(e)
            MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            WITH wallet, wic, count(distinct(e)) AS daos_summoned
            MERGE (wallet)-[edge:_HAS_CONTEXT]->(wic)
            SET edge._count = daos_summoned
            RETURN count(distinct(wallet)) 
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging    
    def connect_dao_admins(self, context):
        query = f"""
            MATCH (wallet:Wallet)-[r:CONTRIBUTOR]->(e:Entity:Snapshot)
            MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            WITH wallet, wic, count(distinct(e)) AS ents
            MERGE (wallet)-[edge:_HAS_CONTEXT]->(wic)
            SET edge._count = ents
            RETURN count(distinct(wallet))                           
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging    
    def connect_nft_subscriptions_admins(self, context):
        query = f"""
            MATCH (wallet:Wallet)-[r:HAS_TOKEN]-(token:Token:Nft:Lock)
            MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            WITH wallet, count(distinct(token)) AS locks, wic, collect("unlock-protocol") AS citation
            MERGE (wallet)-[edge:_HAS_CONTEXT]->(wic)
            SET edge._count = locks 
            SET edge._context = citation
            RETURN count(distinct(wallet))
        """
        count = self.query(query)[0].value()
        return count
