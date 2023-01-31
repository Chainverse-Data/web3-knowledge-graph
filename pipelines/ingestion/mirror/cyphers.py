from tqdm import tqdm
from ...helpers import Cypher
from ...helpers import Constraints
from ...helpers import Indexes
from ...helpers import Queries
from ...helpers import count_query_logging

class MirrorCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()

    def create_constraints(self):
        constraints = Constraints()
        constraints.twitter()
        constraints.wallets()
        constraints.mirror_articles()

    def create_indexes(self):
        indexes = Indexes()
        indexes.twitter()
        indexes.wallets()
        indexes.mirror_articles()

    @count_query_logging
    def create_or_merge_articles(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS articles
                MERGE (article:Mirror:MirrorArticle:Article {{originalContentDigest: articles.original_content_digest}}) 
                ON CREATE set article.uuid = apoc.create.uuid(),
                    article.uri = articles.arweaveTx,
                    article.title = articles.title,
                    article.text = articles.body,
                    article.datePublished = datetime(apoc.date.toISO8601(toInteger(articles.timestamp), 's')),
                    article.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    article.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    article.ingestedBy = "{self.CREATED_ID}"
                ON MATCH set article.title = articles.title,
                    article.body = articles.body,
                    article.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    article.ingestedBy = "{self.UPDATED_ID}"
                return count(article)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_twitter(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS twitter_handles
                    MERGE (twitter:Twitter:Account {{handle: toLower(twitter_handles.twitter_handle)}})
                    ON CREATE set twitter.uuid = apoc.create.uuid(),
                        twitter.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        twitter.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        twitter.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set twitter.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        twitter.ingestedBy = "{self.UPDATED_ID}"
                    return count(twitter)    
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_NFTs(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS NFTs
                    MERGE (nft:Mirror:ERC721 {{address: toLower(NFTs.address)}})
                    ON CREATE set nft.uuid = apoc.create.uuid(),
                        nft.chainId = NFTs.chain_id,
                        nft.supply = NFTs.supply,
                        nft.symbol = NFTs.symbol,
                        nft.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        nft.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        nft.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set nft.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        nft.chainId = NFTs.chainId,
                        nft.supply = NFTs.supply,
                        nft.symbol = NFTs.symbol,
                        nft.ingestedBy = "{self.UPDATED_ID}"
                    return count(nft)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_authors_to_articles(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS articles_authors
                        MATCH (article:Mirror:MirrorArticle:Article {{originalContentDigest: articles_authors.original_content_digest}})
                        MATCH (wallet:Wallet {{address: toLower(articles_authors.author)}})
                        WITH article, wallet
                        MERGE (wallet)-[edge:AUTHOR]->(article)
                        ON CREATE set edge.uuid = apoc.create.uuid(),
                            edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            edge.ingestedBy = "{self.CREATED_ID}"
                        ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            edge.ingestedBy = "{self.UPDATED_ID}"
                        return count(edge)
                """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_NFTs_to_articles(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS NFTs_articles
                        MATCH (article:Mirror:MirrorArticle:Article {{originalContentDigest: NFTs_articles.original_content_digest}})
                        MATCH (nft:Mirror:ERC721 {{address: toLower(NFTs_articles.address)}})
                        WITH article, nft
                        MERGE (article)-[edge:HAS_NFT]->(nft)
                        ON CREATE set edge.uuid = apoc.create.uuid(),
                            edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            edge.ingestedBy = "{self.CREATED_ID}"
                        ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            edge.ingestedBy = "{self.UPDATED_ID}"
                        return count(edge)
                """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_NFTs_to_owners(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS NFTs_articles
                        MATCH (wallet:Wallet {{address: toLower(NFTs_articles.owner)}})
                        MATCH (nft:Mirror:ERC721 {{address: toLower(NFTs_articles.address)}})
                        WITH wallet, nft
                        MERGE (wallet)-[edge:IS_OWNER]->(nft)
                        ON CREATE set edge.uuid = apoc.create.uuid(),
                            edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            edge.ingestedBy = "{self.CREATED_ID}"
                        ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            edge.ingestedBy = "{self.UPDATED_ID}"
                        return count(edge)
                """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_NFTs_to_receipient(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS NFTs_articles
                        MATCH (wallet:Wallet {{address: toLower(NFTs_articles.funding_recipient)}})
                        MATCH (nft:Mirror:ERC721 {{address: toLower(NFTs_articles.address)}})
                        WITH wallet, nft
                        MERGE (wallet)-[edge:IS_RECEIPIENT]->(nft)
                        ON CREATE set edge.uuid = apoc.create.uuid(),
                            edge.createdDt = datetime(apoc.date.toISO8601(
                                apoc.date.currentTimestamp(), 'ms')),
                            edge.lastUpdateDt = datetime(apoc.date.toISO8601(
                                apoc.date.currentTimestamp(), 'ms')),
                            edge.ingestedBy = "{self.CREATED_ID}"
                        ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            edge.ingestedBy = "{self.UPDATED_ID}"
                        return count(edge)
                """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_twitter_to_article(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS twitter_articles
                        MATCH (article:Mirror:MirrorArticle:Article {{originalContentDigest: twitter_articles.original_content_digest}})
                        MATCH (twitter:Twitter:Account {{handle: toLower(twitter_articles.twitter_handle)}})
                        WITH article, twitter, twitter_articles
                        MERGE (article)-[edge:REFERENCES]->(twitter)
                        ON CREATE set edge.uuid = apoc.create.uuid(),
                            edge.mention_count = twitter_articles.mention_count,
                            edge.createdDt = datetime(apoc.date.toISO8601(
                                apoc.date.currentTimestamp(), 'ms')),
                            edge.lastUpdateDt = datetime(apoc.date.toISO8601(
                                apoc.date.currentTimestamp(), 'ms')),
                            edge.ingestedBy = "{self.CREATED_ID}"
                        ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            edge.mention_count = twitter_articles.mention_count,
                            edge.ingestedBy = "{self.UPDATED_ID}"
                        return count(edge)
                """
            count += self.query(query)[0].value()
        return count
