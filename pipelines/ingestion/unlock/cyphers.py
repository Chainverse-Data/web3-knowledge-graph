import pandas as pd 
import logging
from ...helpers import Cypher
from ...helpers import Constraints, Indexes, Queries
from ...helpers import count_query_logging


class UnlockCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()

        ### ingest wallets

    ## should add something to make sure that the number of wallets created / merged matches number of unique
    ## wallets in the file
    def create_wallets(self, urls):
        count = 0
        for url in urls:
            walletsQuery = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as wallets
            MERGE (w:Wallet {{address: wallets.address}})
            ON MATCH SET
                w:UnlockTestStage,
                w.lastUpdateDt = datetime(apoc.date.toISO8601(toInteger(wallets.occurDt), 'ms'))
            ON CREATE SET
                w.uuid = apoc.create.uuid(),
                w:UnlockTestStage,
                w.lastUpdateDt = datetime(apoc.date.toISO8601(toInteger(wallets.occurDt), 'ms')),
                w.createdDt = datetime(apoc.date.toISO8601(toInteger(wallets.occurDt), 'ms'))
            RETURN COUNT(DISTINCT(w))
                """
            count += self.query(walletsQuery)[0].value()
            logging.info(f"Nice I created {count} wallets.")
        return count 
    
    def create_locks(self, urls):
        count = 0
        for url in urls:
            lockMetadataQuery = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as locks
            MERGE (t:Token {{address: locks.address}})  // this needs to change lol
            ON MATCH SET
                t:Nft,
                t:Lock,
                t:UnlockTestStage,
                t.lastUpdateDt = datetime(apoc.date.toISO8601(toInteger(locks.occurDt), 'ms')),
                t.price = locks.price
            ON CREATE SET
                t:Nft,
                t:Lock,
                t.uuid = apoc.create.uuid(),
                t:UnlockTestStage,
                t.lastUpdateDt = datetime(apoc.date.toISO8601(toInteger(locks.occurDt), 'ms')),
                t.createdDt = datetime(apoc.date.toISO8601(toInteger(locks.occurDt), 'ms'))
            RETURN
                COUNT(DISTINCT(t))
                        """
            count += self.query(lockMetadataQuery)[0].value()
            logging.info(f"Nice I created or modified {count} locks.")
        return count
    
    def create_keys(self, urls):
        count = 0
        for url in urls:
            keyMetadataQuery = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as keys
            MERGE (t:Token:Nft {{tokenId: keys.keyId, tokenContract: keys.lockAddress}})
            ON MATCH SET
                t:UnlockTestStage,
                t:Key,
                t:Instance, // should discuss with Xqua w/r/t what this means for NFT ontology
                t.lastUpdateDt = datetime(apoc.date.toISO8601(toInteger(keys.occurDt), 'ms')),
                t.expiration = keys.expiration,
                t.tokenUri = keys.tokenURI,
                t.tokenContract = keys.lockAddress
            ON CREATE SET
                t:UnlockTestStage,
                t:Key,
                t.uuid = apoc.create.uuid(),
                t.lastUpdateDt = datetime(apoc.date.toISO8601(toInteger(keys.occurDt), 'ms')),
                t.createdDt = datetime(apoc.date.toISO8601(toInteger(keys.occurDt), 'ms')),
                t.tokenUri = keys.tokenURI,
                t.tokenContract = keys.lockAddress,
                t.expiration = keys.expiration
            RETURN
                COUNT(DISTINCT(t))
                        """
            count += self.query(keyMetadataQuery)[0].value()
            logging.info(f"Nice I created or modified {count} keys.")
        return count

    def create_lock_managers(self, urls):
        count = 0 
        for url in urls:
            lockManagerQuery = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as lockManagers
            MATCH (t:Token:Nft {{address: lockManagers.lockAddress}})
            MATCH (w:Wallet {{address: lockManagers.lockManager}})
            WITH 
                w,
                t,
                datetime(apoc.date.toISO8601(toInteger(lockManagers.occurDt), 'ms')) as ts
            MERGE
                (w)-[r:HAS_TOKEN]->(t)
            set 
                r.lastUpdateDt = ts
            set
                r.citation = 'Unlock Subgraph'
            set
                r.lastUpdateDt = ts
            RETURN
                COUNT(DISTINCT(r))
                """
            count += self.query(lockManagerQuery)[0].value()
            logging.info(f"Nice I created or modified {count} lock managers.")
        return count

    def create_key_holders(self, urls):
        count = 0 
        for url in urls:
            keyHolderQuery = f"""
                LOAD CSV WITH HEADERS FROM '{url}' as keyHolders
                MATCH (t:Token:Nft:Instance {{tokenId: keyHolders.keyId, tokenContract: keyHolders.lockAddress}})
                MATCH (w:Wallet {{address: keyHolders.owner}})
                WITH t,w, timestamp() as ts
                MERGE (w)-[r:HAS_BALANCE]->(t)
                SET r.lastUpdateDt = ts
                SET r.createdDt = ts
                SET r.citation = 'Unlock Subgraph'
                RETURN COUNT(DISTINCT(w))"""
            count += self.query(keyHolderQuery)[0].value()
            logging.info(f"Nice I created or modified {count} key holders.")
        return count

    def connect_locks_keys(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as lockKeys
            WITH collect(distinct(lockKeys.adddress)) as lockAddresses
            MATCH (t:Token:Nft)
            MATCH (i:Instance:Token:Nft)
            WHERE t.address in lockAddresses
            AND t.address = i.tokenContract
            AND NOT (t)-[:HAS_INSTANCE]->(i)
            WITH t, i
            MERGE (t)-[r:HAS_INSTANCE]->(i)
            SET r.citation = 'Unlock Subgraph'
            RETURN count(distinct(t)) as locks, count(distinct(r)) as rels, count(distinct(i)) as keys"""
            self.query(query)
            count += 1
        logging.info(f"Nice I ran {count} queries to connect locks and keys")

        ## this is fucked up and needs to be fixed
        checkQuery = f"""
        LOAD CSV WITH HEADERS FROM '{url}' as lockKeys
        WITH collect(distinct(lockKeys.adddress)) as lockAddresses
        MATCH (t:Token:Nft)-[r:HAS_INSTANCE]->(i:Instance:Token:Nft)
        RETURN COUNT(DISTINCT(t)) as tokens, COUNT(DISTINCT(r)) as rels, COUNT(DISTINCT(i)) as instances
        """   
        countQuery = self.query(checkQuery)
        locks = countQuery[0].value()
        logging.info(f"Nice I connected  {locks} locks to their keys.")
        return count

            
            




                
                

            
            
            


            



        