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
        logging.info("Creating or merging locks...")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS locks
                    MERGE(lock:Nft:ERC721 {{address: locks.address}})
                    ON CREATE SET lock.uuid = apoc.create.uuid(),
                        lock.name = locks.name,
                        lock.price = locks.price,
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
        logging.info("Creating or merging keys...")
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
        logging.info("Linking or merging managers to locks...")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS managers
                    MATCH (wallet:Wallet {{address: managers.address}}), (lock:Nft:ERC721 {{address: managers.lock}})
                    WITH wallet, lock, managers
                    MERGE (wallet)-[r:CREATED]->(lock)
                    ON CREATE SET r.createdDt =  datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH SET r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    RETURN count(r)
                    """
            count += self.query(query)[0].value()

        return count

    @count_query_logging
    def link_or_merge_locks_to_keys(self, urls):
        logging.info("Linking or merging locks to keys...")
        count = 0
        for url in urls:
            query = f""" 
                    LOAD CSV WITH HEADERS FROM '{url}' as keys
                    MATCH (lock:Nft:ERC721 {{contractAddress: keys.contractAddress}}), (key:Nft:ERC721:Instance {{contractAddress: keys.contractAddress}})
                    WITH lock, key, keys
                    MERGE (lock)-[r:HAS_KEY]->(key)
                    ON CREATE SET r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        r.asOf = keys.asOf
                    ON MATCH SET r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        r.asOf = keys.asOf
                    RETURN count(r)
                    """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_holders_to_locks(self, urls):
        logging.info("Linking or merging holders to locks...")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS holders
                    MATCH (wallet:Wallet {{contractAddress: holders.contractAddress}}), (lock:Nft:ERC721 {{contractAddress holders.contractAddress}})
                    WITH wallet, lock, holders
                    MERGE (wallet)-[r:HOLDS]->(lock)
                    ON CREATE SET r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH SET r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    RETURN count(r)                   
                    """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_holders_to_keys(self, urls):
        logging.info("Linking or merging holders to keys...")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM {url} AS keys
                    MATCH (wallet: Wallet {{contractAddress: keys.contractAddress}}), (key:Nft:ERC721:Instance {{contractAddress: keys.contractAddress}})
                    WITH wallet, key, keys
                    MERGE (wallet)-[r:HOLDS_INSTANCE]->(key)
                    ON CREATE SET r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        r.asOf = keys.asOf
                    ON MATCH SET r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        r.asOf = keys.asOf
                    RETURN count(r)
                    """
            count += self.query(query)[0].value()
        return count
            
            


            



        