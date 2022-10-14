from .cypher import Cypher
from .decorators import count_query_logging
# This file is for universal queries only, any queries that generate new nodes or edges must be in its own cyphers.py file in the service folder

class Queries(Cypher):
    """This class holds queries for general nodes such as Wallet or Twitter"""
    def __init__(self, database=None):
        super().__init__(database)

    @count_query_logging
    def create_wallets(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS admin_wallets
                    MERGE(wallet:Wallet {{address: admin_wallets.address}})
                    ON CREATE set wallet.uuid = apoc.create.uuid(),
                        wallet.address = admin_wallets.address,
                        wallet.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        wallet.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set wallet.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        wallet.address = admin_wallets.address
                    return count(wallet)
            """
            count += self.query(query)[0].value()
        return count
