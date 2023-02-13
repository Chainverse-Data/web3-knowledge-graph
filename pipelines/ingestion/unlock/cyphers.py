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
        print("DEBUG: create_or_merge_locks is running...")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS locks
                    MERGE(Nft:Lock:ERC721 {{address: locks.address}})
                    ON CREATE SET Nft.uuid = apoc.create.uuid(),
                        Nft.name = locks.name,
                        Nft.price = locks.price,
                        Nft.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        Nft.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        Nft.ingestedBy = '{self.CREATED_ID}'
                    ON MATCH SET Nft.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        Nft.ingestedBy = '{self.UPDATED_ID}'
                    RETURN count(Nft)
                    """
            print("DEBUG: Locks Count: {}".format(count))        
            count += self.query(query)[0].value()
        return count
    
    @count_query_logging
    def create_or_merge_keys(self, urls):
        print("DEBUG: create_or_merge_keys is running...")
        count = 0
        for url in urls: 
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS keys
                    MERGE(Nft:Key:ERC721:Instance {{address: keys.address}})
                    ON CREATE SET Nft.uuid = apoc.create.uuid(),
                        Nft.tokenId = keys.id,
                        Nft.expiration = keys.expiration,
                        Nft.tokenUri = keys.tokenUri,
                        Nft.createdAt = keys.createdAt,
                        Nft.network = keys.network,
                        Nft.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        Nft.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        Nft.ingestedBy = '{self.CREATED_ID}'
                    ON MATCH SET Nft.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        Nft.ingestedBy = '{self.UPDATED_ID}'
                    RETURN count(Nft)
                    """
            print("DEBUG: Keys Count: {}".format(count))        
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_managers_to_locks(self, urls):
        print("DEBUG: link_or_merge_managers_to_locks is running...")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS managers
                    MATCH (wallet:Wallet {{address: managers.address}}), (Nft:Lock:ERC721 {{managers.lock}})
                    WITH wallet, Nft, managers
                    MERGE (wallet)-[r:CREATED]->(Nft)
                    ON CREATE SET r.createdDt =  datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    RETURN count(r)
                    """

            print("DEBUG: Managers to Locks Count: {}".format(count))        
            count += self.query(query)[0].value()

        return count

    @count_query_logging
    def link_or_merge_locks_to_keys(self, urls):
        print("DEBUG: link_or_merge_locks_to_keys is running...")
        count = 0
        for url in urls:
            query = f""" 
                    LOAD CSV WITH HEADERS FROM '{url}' as keys
                    MATCH (Nft:Lock:ERC721 {{tokenAddress: keys.address}}), (Nft:Key:ERC721:Instance {{address: keys.address}})
                    WITH Nft:Lock:ERC721, Nft:Key:ERC721:Instance, keys
                    MERGE (Nft:Lock:ERC721)-[r:HAS_KEY]->(Nft:Key:ERC721:Instance)
                    ON CREATE SET r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        r.asOf = keys.asOf,           
                    RETURN count(r)
                    """
            print("DEBUG: Locks to Keys Count: {}".format(count))
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_holders_to_locks(self, urls):
        print("DEBUG: link_or_merge_holders_to_locks is running...")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS holders
                    MATCH (wallet:Wallet {{tokenAddress: holders.tokenAddress}}), (Nft:Lock:ERC721 {{tokenAddress holders.tokenAddress}})
                    WITH wallet, Nft, holders
                    MERGE (wallet)-[r:HOLDS]->(Nft)
                    ON CREATE SET r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    RETURN count(r)                   
                    """
            print("DEBUG: Holders to Locks Count: {}".format(count))
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_holders_to_keys(self, urls):
        print("DEBUG: link_or_merge_holders_to_keys is running...")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM {url} AS keys
                    MATCH (wallet: Wallet {{tokenAddress: keys.address}}), (Nft:Key:ERC721:Instance {{address: keys.address}})
                    WITH wallet, Nft, keys
                    MERGE (wallet)-[r:HOLDS_INSTANCE]->(Nft)
                    ON CREATE SET r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        r.asOf = keys.asOf
                    RETURN count(r)
                    """
            print("DEBUG: Holders to Keys Count: {}".format(count))
            count += self.query(query)[0].value()
        return count
            
            


            



        