import logging
from datetime import datetime
from datetime import timedelta
import os

from neo4j import Record

from ...helpers import Indexes, Constraints
from ...helpers import Cypher
from ...helpers import count_query_logging, get_query_logging

DEBUG = os.environ.get("DEBUG", False)

class TwitterENSBiosCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)
    
    def create_indexes(self):
        index = Indexes()
        index.website()

    def create_constraints(self):
        constraint = Constraints()
        constraint.website()

    @get_query_logging
    def get_bios(self) -> list[Record]:
        query = f"""
            MATCH (twitter:Twitter:Account)
            WHERE NOT twitter.bio IS NULL
            WITH twitter.handle as handle, twitter.bio as bio
            RETURN distinct(handle), bio
        """
        if DEBUG:
            query += " LIMIT 10000"
        results = self.query(query)
        return results

    @count_query_logging
    def link_twitter_ens(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM "{url}" as data
                MATCH (ens:Alias:Ens {{name: toLower(data.ens)}})
                MATCH (twitter:Twitter:Account {{handle: toLower(data.handle)}})
                MERGE (twitter)-[edge:HAS_ALIAS]->(ens)
                SET edge.citation = "From twitter ENS Bio processor"
                SET edge.lastUpdateDt = datetime()
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count