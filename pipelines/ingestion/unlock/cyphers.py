import pandas as pd 
from ...helpers import Cypher
from ...helpers import Constraints, Indexes, Queries
from ...helpers import count_query_logging


class UnlockCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()

        ### ingest wallets


    def create_wallets(self, urls):
        count = 0
        for url in urls:
            walletsQuery = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as wallets
            MERGE (w:Wallet {{address: wallets.address}})
            ON MATCH SET
                w.lastUpdateDt = datetime(apoc.date.toISO8601(toInteger(wallets.occurDt), 'ms'))
            ON CREATE SET
                w.uuid = apoc.create.uuid(),
                w.lastUpdateDt = datetime(apoc.date.toISO8601(toInteger(wallets.occurDt), 'ms')),
                w.createdDt = datetime(apoc.date.toISO8601(toInteger(wallets.occurDt), 'ms'))
            RETURN COUNT(DISTINCT(w))
                """
            count += self.query(walletsQuery)[0].value()
        return count 
    
    def create_locks(self, urls):
        count = None

        return count

            



        