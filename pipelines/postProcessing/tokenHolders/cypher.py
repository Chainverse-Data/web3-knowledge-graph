
from datetime import datetime, timedelta
from tqdm import tqdm
from ...helpers import Cypher, Queries, get_query_logging, count_query_logging
from typing import cast

class TokenHoldersCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)
        self.queries = Queries()

    def create_indexes(self):
        query = "CREATE INDEX tokenIdHeld IF NOT EXISTS FOR ()-[r:HOLDS_TOKEN]-() on (r.tokenId)"
        self.query(query)
        query = "CREATE INDEX holdsNumericBalance IF NOT EXISTS FOR ()-[r:HOLDS]-() ON (r.numericBalance)"
        self.query(query)

    @count_query_logging
    def set_pipeline_status(self, address):
        query = f"""
            MATCH (t:Token {{address: toLower($address)}}) 
            SET t.holderPipelineStatus = "started"
            RETURN count(t)
        """
        count = self.query(query, parameters={"address": address})[0].value()
        return count

    @count_query_logging
    def clean_NFT_token_holding(self, urls) -> int:
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS holdings
                MATCH (wallet:Wallet {{address: toLower(holdings.address)}})
                MATCH (token:Token {{address: toLower(holdings.contractAddress)}})
                MATCH (wallet)-[edge:HOLDS_TOKEN]->(token)
                SET edge.balance = 0
                RETURN count(edge)
            """
            count += cast(int, self.query(query)[0].value())
        return count

    @count_query_logging
    def update_tokens(self, tokens: list[str]) -> int:
        count = 0
        query = """
            MATCH (token:Token)
            WHERE token.address in $tokens
            SET token.lastHoldersUpdateDt = datetime()
            SET token.holderPipelineStatus = "finished"
            RETURN count(token)
        """
        count += cast(int, self.query(query, parameters={"tokens": tokens})[0].value())
        return count

    @count_query_logging
    def link_or_merge_NFT_token_holding(self, urls) -> int:
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS holdings
                MATCH (wallet:Wallet {{address: toLower(holdings.address)}})
                MATCH (token:Token {{address: toLower(holdings.contractAddress)}})
                MERGE (wallet)-[edge:HOLDS_TOKEN {{tokenId: holdings.tokenId}}]->(token)
                SET edge.balance = toIntegerOrNull(holdings.balance),
                    edge.lastUpdateDt = datetime()
                MERGE (wallet)-[edge2:HOLDS]->(token)
                SET edge2.balance = holdings.balance,
                    edge2.toRemove = null,
                    edge2.numericBalance = toFloatOrNull(holdings.balance),
                    edge2.lastUpdateDt = datetime(),
                    edge2.ingestedBy = "{self.UPDATED_ID}"
                RETURN count(edge2)
            """
            count += cast(int, self.query(query)[0].value())
        return count
    
    @count_query_logging
    def link_or_merge_ERC20_token_holding(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS holdings
                MATCH (wallet:Wallet {{address: toLower(holdings.address)}})
                MATCH (token:Token {{address: toLower(holdings.contractAddress)}})
                MERGE (wallet)-[edge:HOLDS]->(token)
                ON CREATE set edge.uuid = apoc.create.uuid(),
                    edge.balance = holdings.balance,
                    edge.numericBalance = toFloatOrNull(holdings.numericBalance),
                    edge.createdDt = datetime(),
                    edge.lastUpdateDt = datetime(),
                    edge.ingestedBy = "{self.CREATED_ID}"
                ON MATCH set edge.lastUpdateDt = datetime(),
                    edge.balance = holdings.balance,
                    edge.numericBalance = toFloatOrNull(holdings.numericBalance),
                    edge.toRemove = null,
                    edge.ingestedBy = "{self.UPDATED_ID}"
                RETURN count(edge)
            """
            count += cast(int, self.query(query)[0].value())
        return count

    @count_query_logging
    def mark_current_hold_edges(self, tokens: list[str]) -> int:
        query = """
            MATCH (token:Token)
            WHERE token.address IN $tokens
            MATCH (wallet:Wallet)-[edge:HOLDS]-(token)
            WHERE edge.toRemove IS NULL
            SET edge.toRemove = true
            RETURN count(edge)
        """
        print(tokens)
        count = cast(int, self.query(query, parameters={"tokens": tokens})[0].value())
        return count 
    
    @count_query_logging
    def move_old_hold_edges_to_held(self, tokens: list[str]) -> int:
        query = """
            MATCH (token:Token)
            WHERE token.address IN $tokens
            MATCH (wallet:Wallet)-[edge:HOLDS]-(token)
            WHERE edge.toRemove IS NOT NULL
            MERGE (wallet)-[newedge:HELD]-(token)
            SET newedge.balance = edge.balance
            SET newedge.numericBalance = toFloatOrNull(edge.numericBalance)
            SET newedge.ingestedBy = "{self.UPDATED_ID}"
            SET newedge.lastUpdateDt = datetime()
            DELETE edge
            RETURN count(newedge)
        """
        count = cast(int, self.query(query, parameters={"tokens": tokens})[0].value())

        query = """
            MATCH (token:Token)
            WHERE token.address IN $tokens
            MATCH (wallet:Wallet)-[edge:HOLDS_TOKEN]-(token)
            WHERE edge.balance = 0
            MERGE (wallet)-[newedge:HELD_TOKEN]-(token)
            SET newedge.tokenId = edge.tokenId
            SET newedge.ingestedBy = "{self.UPDATED_ID}"
            SET newedge.lastUpdateDt = datetime()
            DELETE edge
            RETURN count(newedge)
        """
        count += cast(int, self.query(query, parameters={"tokens": tokens})[0].value())
        return count