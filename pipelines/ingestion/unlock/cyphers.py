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
                t:UnlockTestStage,
                t.lastUpdateDt = datetime(apoc.date.toISO8601(toInteger(locks.occurDt), 'ms')),
                t.price = locks.price
            ON CREATE SET
                t:Nft,
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

            



        