from datetime import datetime
from ...helpers import Cypher
from ...helpers import count_query_logging, get_query_logging


class WebsiteCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    @get_query_logging
    def get_all_twitter(self, interval=10000):
        offset = 0
        results = []

        while True:
            query = f"""
                        MATCH (t:Twitter) 
                        WHERE t.bio IS NOT NULL
                        return t.handle, t.bio
                        SKIP {offset} LIMIT {interval}
                    """
            x = self.query(query)
            if not x:
                break
            results.extend(x)
            offset += interval

        twitter = [(y.get("t.handle"), y.get("t.bio")) for y in results]
        return twitter

    @get_query_logging
    def get_recent_twitter(self, cutoff: datetime, interval=10000):
        offset = 0
        results = []

        while True:

            query = f"""
                        MATCH (t:Twitter) 
                        WHERE t.createdDt >= datetime({{year: {cutoff.year}, month: {cutoff.month}, day: {cutoff.day}}}) AND t.bio IS NOT NULL
                        return t.handle, t.bio
                        SKIP {offset} LIMIT {interval}
                    """
            x = self.query(query)
            if not x:
                break
            results.extend(x)
            offset += interval

        twitter = [(y.get("t.handle"), y.get("t.bio")) for y in results]
        return twitter

    @count_query_logging
    def create_website(self, urls):
        count = 0
        for url in urls:
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS websites
                        MERGE(w:Website {{url: websites.url}})
                        ON CREATE set w.uuid = apoc.create.uuid(),
                            w.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            w.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                        ON MATCH set w.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            w.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                        return count(w)
                """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_twitter_website(self, urls):
        count = 0
        for url in urls:
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS websites
                        MATCH (t:Twitter {{handle: websites.handle}}), (w:Website {{url: websites.url}})
                        MERGE (t)-[r:HAS_WEBSITE]->(w)
                        return count(r)
                """
            count += self.query(query)[0].value()
        return count
