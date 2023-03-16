import pandas as pd
import logging
from ...helpers import Cypher
from ...helpers import Constraints, Indexes, Queries
from ...helpers import count_query_logging


class FollowerCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()

    def create_indexes(self):
        indexes = Indexes()
        indexes.twitter()

    def create_constraints(self):
        constraints = Constraints()
        constraints.twitter()

    @count_query_logging
    def create_twitter(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS twitter
                    MERGE(t:Twitter:Account {{handle: toLower(twitter.handle)}})
                    ON CREATE set t.uuid = apoc.create.uuid(),
                        t.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        t.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set
                        t.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(t)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_twitter_followers(self, urls):
        count = 0
        for url in urls:
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as twitter
                        MATCH (f:Twitter:Account {{handle: toLower(twitter.follower)}})
                        MATCH (e:Twitter:Account {{handle: toLower(twitter.handle)}})
                        MERGE (f)-[r:FOLLOWS]->(e)
                        ON CREATE SET
                            r.asOf = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                        return count(r)
            """
            count += self.query(query)[0].value()
        return count
