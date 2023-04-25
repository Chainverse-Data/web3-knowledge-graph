
from datetime import datetime, timedelta
from tqdm import tqdm
from ...helpers import Cypher, Queries, get_query_logging, count_query_logging
from typing import cast

class CuratedTokenHoldingCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)
        self.queries = Queries()

    def create_indexes(self):
        query = "CREATE INDEX tokenIdHeld IF NOT EXISTS FOR ()-[r:HOLDS_TOKEN]-() on (r.tokenId)"
        self.query(query)
        query = "CREATE INDEX holdsNumericBalance IF NOT EXISTS FOR ()-[r:HOLDS]-() ON (r.numericBalance)"
        self.query(query)

    # @get_query_logging
    # def get_citizen_ERC20_tokens(self, propotion=0.25):
    #     query = f"""
    #         MATCH (w:Wallet)-[r:HOLDS]->(t:Token)
    #         WHERE t:ERC20
    #         WITH t, count(distinct(w)) AS count_holders
    #         WHERE count_holders > 250 
    #         MATCH (w)-[r:HOLDS]->(t:Token:ERC721)
    #         MATCH (w)-[:_HAS_CONTEXT]->(context:_Wic)
    #         WHERE not context:_IncentiveFarming
    #         WITH t, count_holders, count(distinct(w)) AS count_wic
    #         WITH t, tofloat(count_wic) / count_holders AS thres
    #         WHERE thres > {propotion}
    #         RETURN distinct(t.address) AS address
    #     """
    #     tokens = self.query(query)
    #     tokens = [token["address"] for token in tokens]
    #     return tokens

    # @get_query_logging
    # def get_overrepresented_ERC20_tokens(self, propotion=0.25):
    #     query = f"""
    #         MATCH (wallet)-[r:HOLDS]->(token:Token)
    #         WHERE token:ERC20 AND NOT (wallet)-[:HAS_CONTEXT]->(:_Wic:_IncentiveFarming)
    #         WITH count(distinct(wallet)) as citizens
    #         MATCH (wallet)-[r:HOLDS]->(token:Token)
    #         WHERE token:ERC20 AND NOT (wallet)-[:HAS_CONTEXT]->(:_Wic:_IncentiveFarming)
    #         WITH token, apoc.node.degree(token, "HOLDS") AS holders, citizens
    #         WHERE toFloat(holders)/toFloat(citizens) > {propotion}
    #         RETURN distinct(token.address) AS address
    #     """
    #     tokens = self.query(query)
    #     tokens = [token["address"] for token in tokens]
    #     return tokens

    # @get_query_logging
    # def get_verified_ERC20_tokens(self, propotion=0.25):
    #     query = f"""
    #         MATCH (t:Token)
    #         WHERE t:ERC20 AND t.blueCheckmark = true
    #         RETURN distinct(t.address) AS address
    #     """
    #     tokens = self.query(query)
    #     tokens = [token["address"] for token in tokens]
    #     return tokens

    @get_query_logging
    def get_manual_selection_ERC20_tokens(self) -> list[str]:
        query = f"""
            MATCH (t:Token)
            WHERE t:ERC20 AND t.manualSelection IS NOT NULL
            RETURN distinct(t.address) AS address, t.manualSelection as schedule, t.lastHoldersUpdateDt as lastHoldersUpdateDt
        """
        records = self.query(query)
        tokens = []
        for record in records:
            if (record["lastHoldersUpdateDt"]):
                date = record["lastHoldersUpdateDt"]
            else:
                date = datetime.now() - timedelta(days=100)
            tokens.append(
                {
                    "address": record["address"], 
                    "schedule": record["schedule"], 
                    "lastHoldersUpdateDt": date
                }
            )
        return tokens

    # @get_query_logging
    # def get_citizen_NFT_tokens(self, propotion=0.25):
    #     query = f"""
    #         MATCH (w:Wallet)-[r:HOLDS]->(t:Token)
    #         WHERE t:ERC721 OR t:ERC1155
    #         WITH t, count(distinct(w)) AS count_holders
    #         WHERE count_holders > 250 
    #         MATCH (w)-[r:HOLDS]->(t:Token:ERC721)
    #         MATCH (w)-[:_HAS_CONTEXT]->(context:_Wic)
    #         WHERE not context:_IncentiveFarming
    #         WITH t, count_holders, count(distinct(w)) AS count_wic
    #         WITH t, tofloat(count_wic) / count_holders AS thres
    #         WHERE thres > {propotion}
    #         RETURN distinct(t.address) AS address
    #     """
    #     tokens = self.query(query)
    #     tokens = [token["address"] for token in tokens]
    #     return tokens
    
    # @get_query_logging
    # def get_overrepresented_NFT_tokens(self, propotion=0.25):
    #     query = f"""
    #         MATCH (wallet)-[r:HOLDS]->(token:Token)
    #         WHERE (token:ERC721 OR token:ERC1155) AND NOT (wallet)-[:HAS_CONTEXT]->(:_Wic:_IncentiveFarming)
    #         WITH count(distinct(wallet)) as citizens
    #         MATCH (wallet)-[r:HOLDS]->(token:Token)
    #         WHERE (token:ERC721 OR token:ERC1155) AND NOT (wallet)-[:HAS_CONTEXT]->(:_Wic:_IncentiveFarming)
    #         WITH token, apoc.node.degree(token, "HOLDS") AS holders, citizens
    #         WHERE toFloat(holders)/toFloat(citizens) > {propotion}
    #         RETURN distinct(token.address) AS address
    #     """
    #     tokens = self.query(query)
    #     tokens = [token["address"] for token in tokens]
    #     return tokens
    
    # @get_query_logging
    # def get_bluechip_NFT_tokens(self, min_price=10):
    #     query = f"""
    #         MATCH (t:Token)
    #         WHERE (t:ERC721 OR t:ERC1155) AND t.floorPrice > {min_price}
    #         RETURN distinct(t.address) AS address
    #     """
    #     tokens = self.query(query)
    #     tokens = [token["address"] for token in tokens]
    #     return tokens

    # @get_query_logging
    # def get_verified_NFT_tokens(self):
    #     query = f"""
    #         MATCH (t:Token)
    #         WHERE t.safelistRequestStatus = "verified"
    #         RETURN distinct(t.address) AS address
    #     """
    #     tokens = self.query(query)
    #     tokens = [token["address"] for token in tokens]
    #     return tokens
    
    @get_query_logging
    def get_manual_selection_NFT_tokens(self) -> list[str]:
        query = f"""
            MATCH (t:Token)
            WHERE (t:ERC721 OR t:ERC1155) AND t.manualSelection IS NOT NULL
            RETURN distinct(t.address) AS address, t.manualSelection as schedule, t.lastHoldersUpdateDt as lastHoldersUpdateDt
        """
        records = self.query(query)
        tokens = []
        for record in records:
            if (record["lastHoldersUpdateDt"]):
                date = record["lastHoldersUpdateDt"]
            else:
                date = datetime.now() - timedelta(days=100)
            tokens.append(
                {
                    "address": record["address"], 
                    "schedule": record["schedule"], 
                    "lastHoldersUpdateDt": date
                }
            )
        return tokens

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
                    edge.lastUpdatedDt = datetime()
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