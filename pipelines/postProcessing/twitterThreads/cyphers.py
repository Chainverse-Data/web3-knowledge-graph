from datetime import datetime
from datetime import timedelta
import os

from ...helpers import Queries
from ...helpers import Cypher
from ...helpers import count_query_logging, get_query_logging

DEBUG = os.environ.get("DEBUG", False)

class TwitterThreadsCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)
        self.queries = Queries()
    
    def create_indexes(self):
        query = "CREATE INDEX twitterThread IF NOT EXISTS FOR (n:Thread) ON (n.conversationId)"
        self.query(query)
    
    @get_query_logging
    def get_current_twitter_threads(self):
        ingestTime = datetime.now()
        sevendaysAgo = ingestTime - timedelta(days=7)
        query = f"""
            MATCH (thread:Twitter:Thread)
            WHERE thread.createdAt > datetime({{epochSeconds: {int(sevendaysAgo.timestamp())}}})
            RETURN thread.conversationId as conversationId
        """
        if DEBUG:
            query += " LIMIT 10"
        results = self.query(query)
        results = [el["conversationId"] for el in results]
        return results

    @count_query_logging
    def create_or_merge_threads(self, urls):
        count = 0 
        for url in urls:
            query = f"""
                WITH datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as ingestDate
                LOAD CSV WITH HEADERS FROM '{url}' AS threads
                MERGE (thread:Twitter:Thread {{conversationId: threads.conversation_id}})
                ON CREATE SET   thread.uuid = apoc.create.uuid(),
                                thread.createdAt = datetime({{epochSeconds: toInteger(threads.created_at)}}),
                                thread.lastUpdateDt = ingestDate
                ON MATCH SET    thread.createdAt = datetime({{epochSeconds: toInteger(threads.created_at)}}),
                                thread.lastUpdateDt = ingestDate
                RETURN count(thread)
            """

            count += self.query(query)[0].value()
        return count


    @count_query_logging
    def link_or_merge_twitters_address(self, urls):
        count = 0
        for url in urls:
            query = f"""
                WITH datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as ingestDate
                LOAD CSV WITH HEADERS FROM '{url}' as data
                MATCH (twitter:Twitter:Account {{handle: toLower(data.handle)}})
                MATCH (wallet:Wallet {{address: toLower(data.address)}})
                MERGE (wallet)-[edge:HAS_ACCOUNT]->(twitter)
                SET edge.citation = "From twitter thread scraper"
                SET edge.tweets = data.tweets
                SET edge.lastUpdateDt = ingestDate
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_twitters_ens(self, urls):
        count = 0
        for url in urls:
            query = f"""
                WITH datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as ingestDate
                LOAD CSV WITH HEADERS FROM '{url}' as data
                MATCH (twitter:Twitter:Account {{handle: toLower(data.handle)}})
                MATCH (alias:Ens:Alias {{name: toLower(data.name)}})
                MERGE (twitter)-[edge:HAS_ALIAS]->(alias)
                SET edge.citation = "From twitter thread scraper"
                SET edge.tweets = data.tweets
                SET edge.lastUpdateDt = ingestDate
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_thread_authors(self, urls):
        count = 0
        for url in urls:
            query = f"""
                WITH datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as ingestDate
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                MATCH (thread:Twitter:Thread {{conversationId: data.conversation_id}})
                MATCH (account:Twitter:Account {{handle: toLower(data.handle)}})
                MERGE (account)-[edge:AUTHOR]->(thread)
                SET edge.citation = "From twitter thread scraper"
                SET edge.lastUpdateDt = ingestDate
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_thread_replies(self, urls):
        count = 0
        for url in urls:
            query = f"""
                WITH datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as ingestDate
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                MATCH (thread:Twitter:Thread {{conversationId: data.conversation_id}})
                MATCH (account:Twitter:Account {{handle: toLower(data.handle)}})
                MERGE (account)-[edge:REPLIED]->(thread)
                SET edge.citation = "From twitter thread scraper"
                SET edge.lastUpdateDt = ingestDate
                SET edge.twittedAt = datetime({{epochSeconds: toInteger(data.created_at)}})
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count
    