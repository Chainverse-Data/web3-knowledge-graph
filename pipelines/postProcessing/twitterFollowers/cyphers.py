from datetime import datetime
from ...helpers import Cypher
from ...helpers import count_query_logging, get_query_logging
from ...helpers import Queries


class TwitterFollowersCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)
        self.queries = Queries()

    @get_query_logging
    def get_high_rep_handles(self):
        query = """
                    MATCH (w:Wallet)-[:HAS_ALIAS]-(a:Alias)-[:HAS_ALIAS]-(t:Twitter:Account)
                    OPTIONAL MATCH (w)-[:_HAS_CONTEXT]->(context:_Context)
                    WITH t, count(distinct(context)) as reputation
                    RETURN distinct t.userId, t.handle, reputation order by reputation desc
                """
        results = self.query(query)
        return results

    @get_query_logging
    def get_wallet_alias_handles(self, limit=5000):
        results = []
        offset = 0
        while True:
            query = f"""
                        MATCH (w:Wallet)-[:HAS_ALIAS]-(:Alias)-[:HAS_ALIAS]-(t:Twitter:Account)
                        WHERE NOT t:Trash and exists(t.userId)
                        RETURN t
                        SKIP {offset} LIMIT {limit}
                    """
            x = self.query(query)
            if not x:
                break
            results.extend(x)
            offset += limit

        twitters = [y.get("t") for y in results]
        return twitters

    @get_query_logging
    def get_wallet_handles(self, limit=2000):
        results = []
        offset = 0
        while True:
            query = f"""
                        MATCH (w:Wallet)-[:HAS_ACCOUNT]-(t:Twitter:Account)
                        WHERE NOT t:Trash
                        RETURN t
                        SKIP {offset} LIMIT {limit}
                    """
            x = self.query(query)
            if not x:
                break
            results.extend(x)
            offset += limit

        twitter = [y.get("t") for y in results]
        return twitter

    @get_query_logging
    def get_entity_alias_handles(self, limit=2000):
        results = []
        offset = 0
        while True:
            query = f"""
                        MATCH (e:Entity)-[:HAS_ALIAS]-(:Alias)-[:HAS_ALIAS]-(t:Twitter:Account)
                        WHERE NOT t:Trash
                        return t
                        SKIP {offset} LIMIT {limit}
                    """
            x = self.query(query)
            if not x:
                break
            results.extend(x)
            offset += limit

        twitter = [y.get("t") for y in results]
        return twitter

    @get_query_logging
    def get_entity_handles(self, limit=2000):
        results = []
        offset = 0
        while True:
            query = f"""
                        MATCH (e:Entity)-[:HAS_ACCOUNT]-(t:Twitter:Account)
                        WHERE NOT t:Trash
                        return t
                        SKIP {offset} LIMIT {limit}
                    """
            x = self.query(query)
            if not x:
                break
            results.extend(x)
            offset += limit

        twitter = [y.get("t") for y in results]
        return twitter

    @get_query_logging
    def get_token_handles(self, limit=2000):
        results = []
        offset = 0
        while True:
            query = f"""
                        MATCH (t:Token)-[:HAS_ACCOUNT]-(t:Twitter:Account)
                        WHERE NOT t:Trash
                        return t
                        SKIP {offset} LIMIT {limit}
                    """
            x = self.query(query)
            if not x:
                break
            results.extend(x)
            offset += limit

        twitter = [y.get("t") for y in results]
        return twitter

    @count_query_logging
    def create_or_merge_twitter_nodes(self, urls):
        count = self.queries.create_or_merge_twitter(urls)
        return count

    @count_query_logging
    def merge_followers_relationships(self, urls):
        count = 0
        for url in urls:
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as twitter
                        MATCH (f:Twitter:Account {{handle: toLower(twitter.follower)}})
                        MATCH (e:Twitter:Account {{handle: toLower(twitter.handle)}})
                        MERGE (f)-[r:FOLLOWS]->(e)
                        ON CREATE SET
                            r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                        ON MATCH SET
                            r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                        RETURN count(r)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def merge_following_relationships(self, urls):
        count = 0
        for url in urls:
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as twitter
                        MATCH (f:Twitter:Account {{handle: toLower(twitter.handle)}})
                        MATCH (e:Twitter:Account {{handle: toLower(twitter.follower)}})
                        MERGE (f)-[r:FOLLOWS]->(e)
                        ON CREATE SET
                            r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                        ON MATCH SET
                            r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                        RETURN count(r)
            """
            count += self.query(query)[0].value()
        return count
