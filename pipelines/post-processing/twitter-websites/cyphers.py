import sys
import os

from ...helpers import Cypher, Indexes, Queries, Constraints
from ...helpers import count_query_logging, get_query_logging

DEBUG = os.environ.get("DEBUG", False)

class TwitterWebsiteCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)
        self.queries = Queries()

    def create_indexes(self):
        index = Indexes()
        index.website()

    @get_query_logging
    def get_all_twitter(self, interval=10000):
        offset = 0
        results = []
        while True:
            query = f"""
                        MATCH (t:Twitter:Account) 
                        WHERE t.bio IS NOT NULL
                        return t.handle, t.bio
                        SKIP {offset} LIMIT {interval}
                    """
            x = self.query(query)
            if not x:
                break
            results.extend(x)
            offset += interval
            if DEBUG:
                break

        twitter = [(y.get("t.handle"), y.get("t.bio")) for y in results]
        return twitter

    @count_query_logging
    def set_website(self, urls):
        count = 0
        for url in urls:
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS websites
                        MATCH (twitter:Twitter:Account {{handle: websites.handle}})
                        SET twitter.website_bio = websites.url
                        RETURN count(twitter)
                """
            count += self.query(query)[0].value()
        return count