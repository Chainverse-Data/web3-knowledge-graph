
import os
from tqdm import tqdm
from ...helpers import Cypher, Queries, get_query_logging, count_query_logging

DEBUG = os.environ.get("DEBUG", False)

class MirrorNFTCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)
        self.queries = Queries()

    def create_indexes(self):
        query = "CREATE INDEX tokenIdHeld IF NOT EXISTS FOR ()-[r:HOLDS_TOKEN]-() on (r.tokenId)"
        self.query(query)

    @get_query_logging
    def get_mirror_ERC721_tokens(self):
        query = f"""
            MATCH (t:Mirror:Token)
            RETURN distinct(t.address) AS address
        """
        if DEBUG:
            query += "LIMIT 10"
        tokens = self.query(query)
        tokens = [token["address"] for token in tokens]
        return tokens

    @count_query_logging
    def link_or_merge_NFT_token_holding(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS holdings
                MATCH (wallet:Wallet {{address: toLower(holdings.address)}})
                MATCH (token:Token {{address: toLower(holdings.contractAddress)}})
                MERGE (wallet)-[edge:HOLDS_TOKEN {{tokenId: holdings.tokenId}}]->(token)
                SET edge.balance = toIntegerOrNull(holdings.balance),
                    edge.lastUpdateDt = datetime()
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count