
from tqdm import tqdm
from ...helpers import Cypher, Queries, get_query_logging, count_query_logging

class CuratedTokenHoldingCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)
        self.queries = Queries()

    def create_indexes(self):
        query = "CREATE INDEX tokenIdHeld IF NOT EXISTS FOR ()-[r:HOLDS_TOKEN]-() on (r.tokenId)"
        self.query(query)
        query = "CREATE INDEX holdsNumericBalance IF NOT EXISTS FOR ()-[r:HOLDS]-() ON (r.numericBalance)"
        self.query(query)

    @get_query_logging
    def get_citizen_ERC20_tokens(self, propotion=0.25):
        query = f"""
            MATCH (w:Wallet)-[r:HOLDS]->(t:Token)
            WHERE t:ERC20
            WITH t, count(distinct(w)) AS count_holders
            WHERE count_holders > 250 
            MATCH (w)-[r:HOLDS]->(t:Token:ERC721)
            MATCH (w)-[:_HAS_CONTEXT]->(context:_Wic)
            WHERE not context:_IncentiveFarming
            WITH t, count_holders, count(distinct(w)) AS count_wic
            WITH t, tofloat(count_wic) / count_holders AS thres
            WHERE thres > {propotion}
            RETURN distinct(t.address) AS address
        """
        tokens = self.query(query)
        tokens = [token["address"] for token in tokens]
        return tokens

    @get_query_logging
    def get_overrepresented_ERC20_tokens(self, propotion=0.25):
        query = f"""
            MATCH (wallet)-[r:HOLDS]->(token:Token)
            WHERE token:ERC20 AND NOT (wallet)-[:HAS_CONTEXT]->(:_Wic:_IncentiveFarming)
            WITH count(distinct(wallet)) as citizens
            MATCH (wallet)-[r:HOLDS]->(token:Token)
            WHERE token:ERC20 AND NOT (wallet)-[:HAS_CONTEXT]->(:_Wic:_IncentiveFarming)
            WITH token, apoc.node.degree(token, "HOLDS") AS holders, citizens
            WHERE toFloat(holders)/toFloat(citizens) > {propotion}
            RETURN distinct(token.address) AS address
        """
        tokens = self.query(query)
        tokens = [token["address"] for token in tokens]
        return tokens

    @get_query_logging
    def get_verified_ERC20_tokens(self, propotion=0.25):
        query = f"""
            MATCH (t:Token)
            WHERE t:ERC20 AND t.blueCheckmark = true
            RETURN distinct(t.address) AS address
        """
        tokens = self.query(query)
        tokens = [token["address"] for token in tokens]
        return tokens

    @get_query_logging
    def get_manual_selection_ERC20_tokens(self):
        query = f"""
            MATCH (t:Token)
            WHERE t:ERC20 AND t.manualSelection
            RETURN distinct(t.address) AS address
        """
        tokens = self.query(query)
        tokens = [token["address"] for token in tokens]
        return tokens

    @get_query_logging
    def get_citizen_NFT_tokens(self, propotion=0.25):
        query = f"""
            MATCH (w:Wallet)-[r:HOLDS]->(t:Token)
            WHERE t:ERC721 OR t:ERC1155
            WITH t, count(distinct(w)) AS count_holders
            WHERE count_holders > 250 
            MATCH (w)-[r:HOLDS]->(t:Token:ERC721)
            MATCH (w)-[:_HAS_CONTEXT]->(context:_Wic)
            WHERE not context:_IncentiveFarming
            WITH t, count_holders, count(distinct(w)) AS count_wic
            WITH t, tofloat(count_wic) / count_holders AS thres
            WHERE thres > {propotion}
            RETURN distinct(t.address) AS address
        """
        tokens = self.query(query)
        tokens = [token["address"] for token in tokens]
        return tokens
    
    @get_query_logging
    def get_overrepresented_NFT_tokens(self, propotion=0.25):
        query = f"""
            MATCH (wallet)-[r:HOLDS]->(token:Token)
            WHERE (token:ERC721 OR token:ERC1155) AND NOT (wallet)-[:HAS_CONTEXT]->(:_Wic:_IncentiveFarming)
            WITH count(distinct(wallet)) as citizens
            MATCH (wallet)-[r:HOLDS]->(token:Token)
            WHERE (token:ERC721 OR token:ERC1155) AND NOT (wallet)-[:HAS_CONTEXT]->(:_Wic:_IncentiveFarming)
            WITH token, apoc.node.degree(token, "HOLDS") AS holders, citizens
            WHERE toFloat(holders)/toFloat(citizens) > {propotion}
            RETURN distinct(token.address) AS address
        """
        tokens = self.query(query)
        tokens = [token["address"] for token in tokens]
        return tokens
    
    @get_query_logging
    def get_bluechip_NFT_tokens(self, min_price=10):
        query = f"""
            MATCH (t:Token)
            WHERE (t:ERC721 OR t:ERC1155) AND t.floorPrice > {min_price}
            RETURN distinct(t.address) AS address
        """
        tokens = self.query(query)
        tokens = [token["address"] for token in tokens]
        return tokens

    @get_query_logging
    def get_verified_NFT_tokens(self):
        query = f"""
            MATCH (t:Token)
            WHERE t.safelistRequestStatus = "verified"
            RETURN distinct(t.address) AS address
        """
        tokens = self.query(query)
        tokens = [token["address"] for token in tokens]
        return tokens
    
    @get_query_logging
    def get_manual_selection_NFT_tokens(self):
        query = f"""
            MATCH (t:Token)
            WHERE (t:ERC721 OR t:ERC1155) AND t.manualSelection
            RETURN distinct(t.address) AS address
        """
        tokens = self.query(query)
        tokens = [token["address"] for token in tokens]
        return tokens

    @count_query_logging
    def clean_NFT_token_holding(self, urls):
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
            count += self.query(query)[0].value()
        return count

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
                    edge.lastUpdatedDt = datetime()
                MERGE (wallet)-[edge2:HOLDS]->(token)
                SET edge2.balance = holdings.balance,
                    edge2.numericBalance = toFloatOrNull(holdings.numericBalance),
                    edge2.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    edge2.ingestedBy = "{self.UPDATED_ID}"
                RETURN count(edge2)
            """
            count += self.query(query)[0].value()
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
                    edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    edge.ingestedBy = "{self.CREATED_ID}"
                ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    edge.balance = holdings.balance,
                    edge.numericBalance = toFloatOrNull(holdings.numericBalance),
                    edge.ingestedBy = "{self.UPDATED_ID}"
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count