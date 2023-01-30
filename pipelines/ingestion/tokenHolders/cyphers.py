from tqdm import tqdm
from ...helpers import Cypher
from ...helpers import Constraints
from ...helpers import Indexes
from ...helpers import Queries
from ...helpers import count_query_logging

class TokenHoldersCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)
        self.queries = Queries()

    def create_constraints(self):
        constraints = Constraints()
        constraints.tokens()

    def create_indexes(self):
        indexes = Indexes()
        indexes.tokens()
    
    def create_or_merge_tokens(self, urls, token_type):
        "CSV Must have the columns: [contractAddress, symbol, decimal]"
        self.queries.create_or_merge_tokens(urls, token_type)

    @count_query_logging
    def link_wallet_tokens(self, urls):
        "CSV Must have the columns: [contractAddress, address, balance, numericBalance]"
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS holdings
                MATCH (token:Token {{address: toLower(holdings.contractAddress)}}), (wallet:Wallet {{address: toLower(holdings.address)}})
                WITH token, wallet, holdings
                MERGE (wallet)-[edge:HOLDS]->(token)
                ON CREATE set edge.uuid = apoc.create.uuid(),
                    edge.balance = holdings.balance,
                    edge.numericBalance = toFloat(holdings.numericBalance),
                    edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    edge.ingestedBy = "{self.CREATED_ID}"
                ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    edge.balance = holdings.balance,
                    edge.numericBalance = toFloat(holdings.numericBalance),
                    edge.ingestedBy = "{self.UPDATED_ID}"
                return count(edge)
            """
            count += self.query(query)[0].value()
        return count
