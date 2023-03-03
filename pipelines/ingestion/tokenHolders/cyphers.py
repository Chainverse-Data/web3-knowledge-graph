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
                    edge.numericBalance = toFloatOrNull(holdings.numericBalance),
                    edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    edge.ingestedBy = "{self.CREATED_ID}"
                ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    edge.balance = holdings.balance,
                    edge.numericBalance = toFloatOrNull(holdings.numericBalance),
                    edge.ingestedBy = "{self.UPDATED_ID}"
                return count(edge)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_transfers(self, urls):
        "CSV Must have the columns: [from, to, contractAddress, value, hash, erc721TokenId, erc1155Metadata, asset]"
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS transfers
                MATCH (from:Wallet {{address: toLower(transfers.from)}}), (to:Wallet {{address: toLower(transfers.to)}})
                WITH from, to, transfers
                MERGE (from)-[edge:TRANSFERED]->(to)
                ON CREATE set edge.uuid = apoc.create.uuid(),
                    edge.value = toFloat(transfers.value),
                    edge.hash = transfers.hash,
                    edge.contractAddress = transfers.contractAddress,
                    edge.erc721TokenId = transfers.erc721TokenId,
                    edge.erc1155Metadata = transfers.erc1155Metadata,
                    edge.asset = transfers.asset,
                    edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    edge.ingestedBy = "{self.CREATED_ID}"
                ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    edge.value = toFloat(transfers.value),
                    edge.hash = transfers.hash,
                    edge.contractAddress = transfers.contractAddress,
                    edge.erc721TokenId = transfers.erc721TokenId,
                    edge.erc1155Metadata = transfers.erc1155Metadata,
                    edge.asset = transfers.asset,
                    edge.ingestedBy = "{self.UPDATED_ID}"
                return count(edge)
            """
            count += self.query(query)[0].value()
        return count