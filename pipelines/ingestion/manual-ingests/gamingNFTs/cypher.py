from ....helpers import count_query_logging
from ....helpers import Queries
from ....helpers import Cypher

class GamingNFTCyphers(Cypher):
    def __init__(self, database=None):
        self.queries = Queries()
        super().__init__(database)

    @count_query_logging
    def link_or_merge_holdings(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS holdings
                MATCH (wallet:Wallet {{address: toLower(holdings.address)}})
                MATCH (token:Token {{address: toLower(holdings.contract)}})
                MERGE (wallet)-[r:HOLDS]->(token)
                SET r.balance = holdings.balance
                SET r.numericBalance = toInteger(holdings.balance)
                RETURN count(r)
            """
            count += self.query(query)[0].value()
        return count