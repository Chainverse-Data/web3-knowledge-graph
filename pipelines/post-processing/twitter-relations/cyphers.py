import logging
from datetime import datetime
from datetime import timedelta
import os

from ...helpers import Indexes, Constraints
from ...helpers import Cypher
from ...helpers import count_query_logging, get_query_logging

DEBUG = os.environ.get("DEBUG", False)

class TwitterRelationsCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)
    
    def create_indexes(self):
        index = Indexes()
        index.website()

    def create_constraints(self):
        constraint = Constraints()
        constraint.website()

    @get_query_logging
    def get_bios(self):
        ingestTime = datetime.now()
        oneMonthAgo = ingestTime - timedelta(days=30)
        query = f"""
            MATCH (twitter:Twitter)
            WHERE NOT twitter.bio IS NULL
            AND twitter.lastProcessingDateBio IS NULL
            RETURN distinct twitter.handle as handle, twitter.bio as bio 
            UNION 
            MATCH (twitter:Twitter)
            WHERE NOT twitter.bio IS NULL 
            AND twitter.lastProcessingDateBio > datetime({{epochSeconds: {int(oneMonthAgo.timestamp())}}})
            WITH twitter.handle as handle, twitter.bio as bio
            RETURN distinct handle, bio
        """
        if DEBUG:
            query += " LIMIT 1000"
        results = self.query(query)
        return results

    @count_query_logging
    def create_metionned_handles(self, urls):
        count = 0
        for url in urls:
            query = f"""
                WITH datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as ingestDate
                LOAD CSV WITH HEADERS FROM '{url}' AS references 
                MERGE (account:Twitter {{handle: references.metionned_handle}})
                ON CREATE SET account.createdDt = ingestDate,
                    account.uuid = apoc.create.uuid(),
                    account.lastUpdateDt = ingestDate
                ON MATCH SET account.lastUpdateDt = ingestDate
                RETURN count(account)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def ingest_references(self, urls):
        count = 0
        for url in urls:
            query = f"""
                WITH datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as ingestDate
                LOAD CSV WITH HEADERS FROM '{url}' AS references 
                MATCH (account:Twitter {{handle: references.handle}})
                MATCH (mentionned:Twitter {{handle: references.metionned_handle}})
                SET account.lastProcessingDateBio = ingestDate
                WITH account, mentionned, references, ingestDate
                MERGE (account)-[edge:BIO_MENTIONED]->(mentionned)
                ON CREATE SET account.createdDt = ingestDate,
                    account.lastUpdateDt = ingestDate
                ON MATCH SET account.lastUpdateDt = ingestDate
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count 

    ## collects twitter accounts with websites that have no link between twitter & website
    @get_query_logging
    def get_twitter_websites(self):
        query = """
            MATCH (t:Twitter)
            WHERE t.website is not null 
            AND NOT (t)-[:HAS_WEBSITE]->(:Website)
            RETURN distinct t.handle as handle, t.website as website
        """
        results = self.query(query)
        return results 

    @count_query_logging
    def create_domains(self, urls):
        count = 0 
        for url in urls: 
            domains_query = f"""
            WITH datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as ingestDate
            LOAD CSV WITH HEADERS FROM "{url}" as websites
            MERGE (domain:Domain {{domain: websites.domain}})
            ON MATCH SET domain.lastUpdateDt = ingestDate
            ON CREATE SET domain.createdDt = ingestDate,
                          domain.lastUpdateDt = ingestDate,
                          domain.uuid = apoc.create.uuid()
            RETURN COUNT(DISTINCT(domain))
            """
            count += self.query(domains_query)[0].value()
        return count 

    @count_query_logging
    def create_websites(self, urls):
        count = 0 
        for url in urls: 
            websites_query = f"""
            WITH datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as ingestDate
            LOAD CSV WITH HEADERS FROM "{url}" as websites
            MERGE (website:Website {{url: websites.url}})
            ON MATCH SET website.lastUpdateDt = ingestDate
            ON CREATE SET website.createdDt = ingestDate,
                          website.uuid = apoc.create.uuid()
            RETURN COUNT(DISTINCT(website))
            """
            count += self.query(websites_query)[0].value()
            
        return count 

    @count_query_logging
    def link_websites_domains(self, urls):
        count = 0 
        for url in urls:
            link_website_domain_query = f"""
            WITH datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as ingestDate
            LOAD CSV WITH HEADERS FROM "{url}" as websites
            MATCH (website:Website {{url: websites.url}})
            MATCH (domain:Domain {{domain: websites.domain}})
            WITH website, domain, ingestDate
            MERGE (website)-[r:HAS_DOMAIN]->(domain)
            SET r.createdDt = ingestDate
            SET r.lastUpdateDt = ingestDate
            RETURN COUNT(DISTINCT(website))"""

            count += self.query(link_website_domain_query)[0].value()

        return count 

    @count_query_logging
    def link_twitter_websites(self, urls):
        count = 0
        for url in urls:
            query = f"""
                WITH datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as ingestDate
                LOAD CSV WITH HEADERS FROM "{url}" as websites
                MATCH (website:Website {{url: websites.url}})
                MATCH (twitter:Twitter {{handle: websites.handle}})
                MERGE (twitter)-[edge:HAS_WEBSITE]->(website)
                ON CREATE SET edge.createdDt = ingestDate,
                              edge.lastUpdateDt = ingestDate
                ON MATCH SET edge.lastUpdateDt = ingestDate
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count