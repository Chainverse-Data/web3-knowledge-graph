import logging
from ...helpers import Cypher
from ...helpers import Constraints, Indexes, Queries
from ...helpers import count_query_logging
import sys


class UnlockCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()
    
    #TODO: Ask leo about these again - what are these used for? 
    @count_query_logging
    def create_unlock_managers_wallets(self, urls):
        count = self.queries.create_wallets(urls)
        return count 

    @count_query_logging
    def create_unlock_holders_wallets(self, urls):
        count = self.queries.create_wallets(urls)
        return count 
    

    @count_query_logging
    def create_or_merge_locks(self, urls):
        print("create_or_merge_locks is running...")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS locks
                    MERGE(lockNode:Unlock:Lock {{address: "locks.address"}})
                    ON CREATE SET lockNode.uuid = apoc.create.uuid(),
                        lockNode.name = locks.name,
                        lockNode.tokenAddress = locks.tokenAddress,
                        lockNode.creationBlock = locks.creationBlock,
                        lockNode.price = locks.price,
                        lockNode.expirationDuration = locks.expirationDuration,
                        lockNode.totalSupply = toInteger(locks.totalSupply),
                        lockNode.network = locks.network,
                        lockNode.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        lockNode.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        lockNode.asOf = locks.asOf,
                        lockNode.ingestedBy = '{self.CREATED_ID}'
                    ON MATCH SET lockNode.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        lockNode.asOf = locks.asOf,
                        lockNode.ingestedBy = '{self.UPDATED_ID}'
                    RETURN count(lockNode)
                    """
            print("Locks Count: {}".format(count))        
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_managers(self, urls):
        print("create_or_merge_managers is running...")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS managers
                    MERGE(managerNode:Unlock:Manager {{address: managers.address}})
                    ON CREATE SET managerNode.uuid = apoc.create.uuid(),
                        managerNode.lock = managers.lock,
                        managerNode.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        managerNode.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        managerNode.asOf = self.asOf,
                        managerNode.ingestedBy = '{self.CREATED_ID}'
                    ON MATCH SET managerNode.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        managerNode.asOf = self.asOf,
                        managerNode.ingestedBy = '{self.UPDATED_ID}'
                    RETURN count(managerNode)            
                    """
            print("Managers Count: {}".format(count))        
            count += self.query(query)[0].value()
        return count
    
    @count_query_logging
    def create_or_merge_keys(self, urls):
        print("create_or_merge_keys is running...")
        count = 0
        for url in urls: 
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS keys
                    MERGE(keyNode:Unlock:Key {{address: keys.address}})
                    ON CREATE SET keyNode.uuid = apoc.create.uuid(),
                        keyNode.id = keys.id,
                        keyNode.expiration = keys.expiration,
                        keyNode.tokenURI = keys.tokenURI,
                        keyNode.createdAt = keys.createdAt,
                        keyNode.network = keys.network,
                        keyNode.asOf = keys.asOf,
                        keyNode.createDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        keyNode.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        keyNode.ingestedBy = '{self.CREATED_ID}'
                    ON MATCH SET keyNode.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        keyNode.asOf = keys.asOf,
                        keyNode.ingestedBy = '{self.UPDATED_ID}'
                    RETURN count(keyNode)
                    """
            print("Keys Count: {}".format(count))        
            count += self.query(query)[0].value()
        return count
    
    @count_query_logging
    def create_or_merge_holders(self, urls):
        print("create_or_merge_holders is running...")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM {url} AS holders
                    MERGE (holderNode:Unlock:Holder {{address: holders.address}})
                    ON CREATE SET holderNode.uuid = apoc.create.uuid(),
                        holderNode.keyId = holders.keyId,
                        holderNode.tokenAddress = holders.tokenAddress,
                        holderNode.createDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        holderNode.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        holderNode.asOf = holders.asOf,
                        holderNode.ingestedBy = '{self.CREATED_ID}'
                    ON MATCH SET holderNode.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        holderNode.asOf = holders.asOf,
                        holderNode.ingestedBy = '{self.UPDATED_ID}'
                    RETURN count(holderNode)
                    """
            print("Holders Count: {}".format(count))        
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_managers_to_locks(self, urls):
        print("link_or_merge_managers_to_locks is running...")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM {url} AS managers
                    MATCH (managerNode:Unlock:Manager), (lockNode:Unlock:Lock)
                    WHERE managerNode.lock = lockNode.tokenAddress
                    MERGE (managerNode)-[link.CREATED]->(lockNode)
                    ON CREATE SET link.uuid = apoc.create.uuid(),
                        link.asOf = managers.asOf,
                        link.createDt =  datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.lastUpdate = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH SET link.asOf = managers.asOf,
                        link.lastUpdate = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.ingestedBy = "{self.UPDATED_ID}"
                    RETURN count(link)
                    """
            print("Managers to Locks Count: {}".format(count))        
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_locks_to_keys(self, urls):
        print("DEBUG: link_or_merge_locks_to_keys is running...")
        count = 0
        for url in urls:
            # TODO: Ask leo about asOf to ensure correct
            query = f""" 
                    LOAD CSV WITH HEADERS FROM {url} AS locks
                    MATCH (lockNode:Unlock:Lock), (keyNode:Unlock:Key)
                    WHERE lockNode.tokenAddress = keyNode.address
                    MERGE (lockNode)-[link:HAS_KEY]->(keyNode)
                    ON CREATE SET link.uuid = apoc.create.uuid(),  
                        link.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimeStamp(), 'ms')),
                        link.asOf = locks.asOf,           
                        link.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH SET link.asOf = locks.asOf,
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimeStamp(), 'ms')),
                        link.ingestedBy = "{self.UPDATED_ID}"
                    RETURN count(link)
                    """
            print("Locks to Keys Count: {}".format(count))
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_holders_to_locks(self, urls):
        print("DEBUG: link_or_merge_holders_to_locks is running...")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM {url} AS holders
                    MATCH (holderNode:Unlock:Holder), (lockNode:Unlock:Lock)
                    WHERE holderNode.tokenAddress = lockNode.tokenAddresss
                    MERGE (holderNode)-[link:HOLDS]->(lockNode)
                    ON CREATE SET link.uuid = apoc.create.uuid(),
                        link.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.asOf = holders.asOf,
                        link.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH SET link.asOf = holders.asOf,
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.ingestedBy = "{self.UPDATED_ID}"
                    RETURN count(link)                      
                    """
            print("Holders to Locks Count: {}".format(count))
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_holders_to_keys(self, urls):
        print("link_or_merge_holders_to_keys is running...")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM {url} AS holders
                    MATCH (holderNode:Unlock:Holder), (keyNode:Unlock:Key)
                    WHERE holderNode.keyId = keyNode.id
                    MERGE (holderNode)-[link:HOLDS_INSTANCE]->(keyNode)
                    ON CREATE SET link.uuid = apoc.create.uuid(),
                        link.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.asOf = holders.asOf,
                        link.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH SET link.asOf = holders.asOf,
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.ingestedBy = "{self.UPDATED_ID}"
                    RETURN count(link)
                    """
            print("Holders to Keys Count: {}".format(count))
            count += self.query(query)[0].value()
        return count
            
            


            



        