from tqdm import tqdm

from ...helpers import Queries
from ...helpers import Cypher
from ...helpers import count_query_logging, get_query_logging
import os
DEBUG = os.environ.get("DEBUG", False)

class TokenMetadataCyphers(Cypher):
    def __init__(self, database=None):
        self.queries = Queries()
        super().__init__(database)

    @get_query_logging
    def get_empty_ERC20_tokens(self):
        token_node_query = f"""
            MATCH (token:Token:ERC20) 
            WHERE token.address IS NOT NULL AND token.metadataScraped IS NULL
            RETURN token 
        """
        if DEBUG:
            token_node_query += " LIMIT 100"
        tokens = self.query(token_node_query)
        tokens = [dict(token.get('token')) for token in tokens]
        return tokens
    
    @get_query_logging
    def get_empty_ERC721_tokens(self):
        token_node_query = f"""
            MATCH (token:Token)
            WHERE (token:ERC721 OR token:ERC1155) AND token.metadataScraped IS NULL 
            RETURN token
        """
        if DEBUG:
            token_node_query += " LIMIT 100"
        tokens = self.query(token_node_query)
        tokens = [dict(token.get('token')) for token in tokens]
        return tokens

    @count_query_logging
    def add_ERC20_token_node_metadata(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS tokens
                MATCH (t:Token:ERC20 {{address: tokens.address}})
                SET t.metadataScraped = tokens.metadataScraped,
                    t.name = tokens.name,
                    t.symbol = tokens.symbol,
                    t.decimals = toInteger(tokens.decimals),
                    t.logo = tokens.logo,
                    t.symbol = tokens.symbol,
                    t.totalSupply = tokens.totalSupply,
                    t.blueCheckmark = toBooleanOrNull(tokens.blueCheckmark),
                    t.description = tokens.description,
                    t.website = tokens.website,
                    t.email = tokens.email,
                    t.blog = tokens.blog,
                    t.reddit = tokens.reddit,
                    t.slack = tokens.slack,
                    t.facebook = tokens.facebook,
                    t.twitter = tokens.twitter,
                    t.bitcointalk = tokens.bitcointalk,
                    t.github = tokens.github,
                    t.telegram = tokens.telegram,
                    t.wechat = tokens.wechat,
                    t.linkedin = tokens.linkedin,
                    t.discord = tokens.discord,
                    t.whitepaper = tokens.whitepaper,
                    t.tokenPriceUSD = toFloatOrNull(tokens.tokenPriceUSD),
                    t.lastUpdatedDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                return count(t)"""
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def add_ERC721_token_node_metadata(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS tokens
                MATCH (token:Token:ERC721 {{address: tokens.address}})
                SET token.metadataScraped = tokens.metadataScraped,
                    token.title = tokens.title,
                    token.description = tokens.description,
                    token.tokenUri_gateway = tokens.tokenUri_gateway,
                    token.tokenUri_raw = tokens.tokenUri_raw,
                    token.image = tokens.image,
                    token.timeLastUpdated = tokens.timeLastUpdated,
                    token.symbol = tokens.symbol,
                    token.totalSupply = tokens.totalSupply,
                    token.contractDeployer = tokens.contractDeployer,
                    token.deployedBlockNumber = toIntegerOrNull(tokens.deployedBlockNumber),
                    token.floorPrice = toFloatOrNull(tokens.floorPrice),
                    token.collectionName = tokens.collectionName,
                    token.safelistRequestStatus = tokens.safelistRequestStatus,
                    token.imageUrl = tokens.imageUrl,
                    token.openSeaName = tokens.openSeaName,
                    token.openSeaDescription = tokens.openSeaDescription,
                    token.externalUrl = tokens.externalUrl,
                    token.twitterUsername = tokens.twitterUsername,
                    token.lastUpdatedDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                return count(token)"""
            count += self.query(query)[0].value()
        return count
    
    @count_query_logging
    def add_ERC721_deployers(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' as tokens
                MATCH (token:Token:ERC721 {{address: tokens.address}})
                MATCH (deployer:Wallet {{address: toLower(tokens.contractDeployer)}})
                MERGE (deployer)-[edge:DEPLOYED]->(token)
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count
    
    @count_query_logging
    def create_or_merge_socials(self, urls, labels, node_property, data_property, edge, citation):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                MERGE (social:{':'.join(labels)} {{{node_property}: toLower(data.{data_property})}})
                WITH social, data
                MATCH (token:Token {{address: toLower(data.address)}})
                MERGE (token)-[edge:{edge}]->(social)
                SET edge.citation = data.{citation}
                RETURN count(social)
            """
            count += self.query(query)[0].value()
        return count