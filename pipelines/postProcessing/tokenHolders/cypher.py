
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

    @count_query_logging
    def add_NFT_token_node_metadata(self, metadata):
        query = f"""
            UNWIND $data as data
            MATCH (token:Token {{address: data.address}})
            SET token.title = data.title,
                token.description = data.description,
                token.tokenUri_gateway = data.tokenUri_gateway,
                token.tokenUri_raw = data.tokenUri_raw,
                token.image = data.image,
                token.timeLastUpdated = data.timeLastUpdated,
                token.symbol = data.symbol,
                token.totalSupply = data.totalSupply,
                token.contractDeployer = data.contractDeployer,
                token.deployedBlockNumber = toIntegerOrNull(data.deployedBlockNumber),
                token.floorPrice = toFloatOrNull(data.floorPrice),
                token.name = data.collectionName,
                token.safelistRequestStatus  = data.safelistRequestStatus,
                token.imageUrl = data.imageUrl,
                token.openSeaName = data.openSeaName,
                token.openSeaDescription = data.openSeaDescription,
                token.externalUrl = data.externalUrl,
                token.twitterUsername = data.twitterUsername,
                token.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
            return count(token)"""
        count = self.query(query, parameters={"data": [metadata]})[0].value()
        return count

    @count_query_logging
    def add_ERC20_token_node_metadata(self, metadata):
        query = f"""
            UNWIND $data as data
            MATCH (t:Token {{address: toLower(data.contractAddress)}})
            SET t.name = data.tokenName,
                t.symbol = data.symbol,
                t.decimals = toInteger(data.divisor),
                t.tokenType = data.tokenType,
                t.totalSupply = data.totalSupply,
                t.blueCheckmark = toBooleanOrNull(data.blueCheckmark),
                t.description = data.description,
                t.website = data.website,
                t.email = data.email,
                t.blog = data.blog,
                t.reddit = data.reddit,
                t.slack = data.slack,
                t.facebook = data.facebook,
                t.twitter = data.twitter,
                t.bitcointalk = data.bitcointalk,
                t.github = data.github,
                t.telegram = data.telegram,
                t.wechat = data.wechat,
                t.linkedin = data.linkedin,
                t.discord = data.discord,
                t.whitepaper = data.whitepaper,
                t.tokenPriceUSD = toFloatOrNull(data.tokenPriceUSD),
                t.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
            return count(t)"""
        count = self.query(query, parameters={"data": [metadata]})[0].value()
        return count