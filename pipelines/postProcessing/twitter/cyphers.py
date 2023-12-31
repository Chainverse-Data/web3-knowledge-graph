from datetime import datetime
from ...helpers import Cypher
from ...helpers import count_query_logging, get_query_logging


class TwitterCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    @count_query_logging
    def clean_twitter_nodes(self):
        query = """
                    MATCH (n:Twitter:Account)
                    SET n:Account
                    RETURN count(n)
                """
        count = self.query(query)[0].value()
        return count

    @get_query_logging
    def get_all_twitter(self, interval=2000):
        offset = 0
        results = []

        while True:
            query = f"""
                        MATCH (t:Twitter:Account) 
                        WHERE NOT t:Trash
                        return t.handle
                        SKIP {offset} LIMIT {interval}
                    """
            x = self.query(query)
            if not x:
                break
            results.extend(x)
            offset += interval

        twitter = [y.get("t.handle") for y in results]
        return twitter

    @get_query_logging
    def get_all_empty_twitter(self, interval=20000):
        offset = 0
        results = []

        while True:
            query = f"""
                        MATCH (t:Twitter:Account) 
                        WHERE NOT t:Trash AND t.name is NULL
                        return t.handle
                        SKIP {offset} LIMIT {interval}
                    """
            x = self.query(query)
            if not x:
                break
            results.extend(x)
            offset += interval
            break

        twitter = [y.get("t.handle") for y in results]
        return twitter

    @get_query_logging
    def get_recent_empty_twitter(self, cutoff: datetime, interval=2000):
        offset = 0
        results = []

        while True:

            query = f"""
                        MATCH (t:Twitter:Account) 
                        WHERE t.lastUpdateDt >= datetime({{year: {cutoff.year}, month: {cutoff.month}, day: {cutoff.day}}}) AND t.name is NULL AND NOT t:Trash
                        return t.handle
                        SKIP {offset} LIMIT {interval}
                    """
            x = self.query(query)
            if not x:
                break
            results.extend(x)
            offset += interval

        twitter = [y.get("t.handle") for y in results]
        return twitter

    @count_query_logging
    def add_trash_labels(self, urls):
        count = 0
        for url in urls:
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS twitter
                        MATCH (t:Twitter:Account {{handle: twitter.handle}})
                        SET t:Trash
                        return count(t)
                    """

            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def add_twitter_node_info(self, urls):
        count = 0
        for url in urls:
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS twitter
                        MATCH (t:Twitter:Account {{handle: twitter.handle}})
                        SET t:Account,
                            t.name = twitter.name,
                            t.bio = twitter.bio,
                            t.followerCount = toInteger(twitter.followerCount),
                            t.verified = toBoolean(twitter.verified),
                            t.userId = twitter.userId,
                            t.website = twitter.website,
                            t.profileImageUrl = twitter.profileImageUrl,
                            t.location = twitter.location,
                            t.language = twitter.language,
                            t.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                        return count(t)
                    """

            count += self.query(query)[0].value()
        return count
