from ...helpers import Cypher
from ...helpers import constraints
from ...helpers import indexes
from ...helpers import count_query_logging

class TokenHoldersCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    def create_constraints(self):
        # unique ERC20
        return super().create_constraints()

    def create_indexes(self):
        return super().create_indexes()
    
    @count_query_logging
    def create_or_merge_tokens(self, urls, token_type):
        "CSV Must have the columns: [contractAddress, symbol, decimal]"
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS tokens
                    MERGE(token:Contract:Token:{token_type} {{address: toLower(tokens.contractAddress)}})
                    ON CREATE set token.uuid = apoc.create.uuid(),
                        token.symbol = tokens.id, 
                        token.decimal = tokens.id, 
                        token.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        token.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        token.ingestedBy = "{self.CREATED_ID}"
                    return count(token)
            """

    @count_query_logging
    def link_wallet_tokens(self, urls):
        "CSV Must have the columns: [contractAddress, address, balance, numericBalance]"
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS holdings
                MATCH (token:Contract:Token {{address: toLower(holdings.contractAddress)}}), (wallet:Wallet {{address: toLower(holdings.address)}})
                WITH token, wallet, holdings
                MERGE (wallet)-[edge:HOLDS]->(token)
                ON CREATE set edge.uuid = apoc.create.uuid(),
                    edge.balance = holdings.balance,
                    edge.numericBalance = holdings.numericBalance,
                    edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    edge.ingestedBy = "{self.CREATED_ID}"
                ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    edge.balance = holdings.balance,
                    edge.numericBalance = holdings.numericBalance,
                    edge.ingestedBy = "{self.UPDATED_ID}"
                return count(edge)
            """
