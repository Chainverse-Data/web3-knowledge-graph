import logging
from ...helpers import Cypher
from ...helpers import Constraints, Indexes, Queries
from ...helpers import count_query_logging
import sys


class UnlockCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()
    
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
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS locks
                    MERGE(lock:Nft:ERC721:Lock {{contractAddress: locks.address}})
                    ON CREATE SET lock.uuid = apoc.create.uuid(),
                        lock.name = locks.name,
                        lock.price = toIntegerOrNull(locks.price),
                        lock.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        lock.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        lock.ingestedBy = '{self.CREATED_ID}'
                    ON MATCH SET lock.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        lock.ingestedBy = '{self.UPDATED_ID}'
                    RETURN count(lock)
                    """

            count += self.query(query)[0].value()

        return count
    
    @count_query_logging
    def create_or_merge_keys(self, urls):
        count = 0
        for url in urls: 
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS keys
                    MERGE(key:Nft:ERC721:Instance {{contractAddress: keys.contractAddress}})
                    ON CREATE SET key.uuid = apoc.create.uuid(),
                        key.tokenId = keys.id,
                        key.expiration = keys.expiration,
                        key.tokenUri = keys.tokenUri,
                        key.createdAt = keys.createdAt,
                        key.network = keys.network,
                        key.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        key.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        key.ingestedBy = '{self.CREATED_ID}'
                    ON MATCH SET key.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        key.ingestedBy = '{self.UPDATED_ID}'
                    RETURN count(key)
                    """

            count += self.query(query)[0].value()

        return count

    @count_query_logging
    def link_or_merge_managers_to_locks(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS managers
                    MATCH (wallet:Wallet {{address: managers.address}})
                    MATCH (lock:Nft:ERC721:Lock {{contractAddress: managers.lock}})
                    WITH wallet, lock
                    MERGE (wallet)-[r:CREATED]->(lock)
                    ON CREATE SET r.createdDt =  datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH SET r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    RETURN count(r)
                    """

            count += self.query(query)[0].value()

        return count

    @count_query_logging
    def link_or_merge_locks_to_keys(self, urls):
        count = 0
        for url in urls:
            query = f""" 
                    LOAD CSV WITH HEADERS FROM '{url}' AS locks
                    MATCH (lock:Nft:ERC721:Lock {{contractAddress: locks.address}})
                    MATCH (key:Nft:ERC721:Instance {{contractAddress: locks.contractAddress}})
                    WITH lock, key
                    MERGE (lock)-[r:HAS_KEY]->(key)
                    ON CREATE SET r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH SET r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    RETURN count(r)
                    """
            
            count += self.query(query)[0].value()

        return count

    @count_query_logging
    def link_or_merge_holders_to_locks(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS holders
                    MATCH (wallet:Wallet {{address: holders.address}})
                    MATCH (lock:Nft:ERC721:Lock {{contractAddress: holders.keyId}})
                    WITH wallet, lock
                    MERGE (wallet)-[r:HOLDS]->(lock)
                    ON CREATE SET r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH SET r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    RETURN count(r)                   
                    """
            
            count += self.query(query)[0].value()

        return count

    @count_query_logging
    def link_or_merge_holders_to_keys(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS holders
                    MATCH (wallet:Wallet {{address: holders.address}})
                    MATCH (key:Nft:ERC721:Instance {{contractAddress: holders.contractAddress}})
                    WITH wallet, key
                    MERGE (wallet)-[r:HOLDS_INSTANCE]->(key)
                    ON CREATE SET r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH SET r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    RETURN count(r)
                    """
            
            count += self.query(query)[0].value()

        return count

        
