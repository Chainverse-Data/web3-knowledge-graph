import logging
from datetime import datetime
from datetime import timedelta
import pandas as pd

from ...helpers import Indexes, Constraints
from ...helpers import Cypher
from ...helpers import count_query_logging


class TwitterRelsCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)
    
    def create_indexes(self):
        index = Indexes()
        index.website()

    def create_constraints(self):
        constraint = Constraints()
        constraint.website()

    def get_bios(self):
        ingestTime = datetime.now()
        oneMonthAgo = ingestTime - timedelta(days=30)
        query = f"""
        MATCH (twitter:Twitter)
        WHERE NOT twitter.bio IS NULL
        AND twitter.lastProcessingDateBio IS NULL
        RETURN distinct twitter.userId as userId, twitter.bio as bio 
        UNION 
        MATCH (twitter:Twitter)
        WHERE twitter.bio IS NULL 
        AND twitter.lastProcessingDateBio > datetime(epochMillis: {oneMonthAgo.timestamp()})
        WITH twitter.userId as userId, twitter.bio as bio
        RETURN distinct userId, bio
        """
        results = pd.DataFrame(query)
        return results

    def get_websites(self):
        ingestTime = datetime.now()
        twoMonthsAgo = ingestTime - timedelta(days=60)
        query = f"""
        MATCH (twitter:Twitter)
        WHERE NOT twitter.website IS NULL
        AND twitter.lastProcesingDateWebsite IS NULL
        RETURN distinct twitter.userId as userId, twitter.website as website 
        UNION 
        MATCH (twitter:Twitter)
        WHERE twitter.website IS NULL 
        AND twitter.lastProcessingDateWebsite > datetime(epochMillis: {twoMonthsAgo.timestamp()})
        WITH twitter.userId as userId, twitter.website as website
        RETURN distinct userId, website
        """
        results = pd.DataFrame(query)
        return results
    
    def delete_lame_rels(self):
        query = """
        call apoc.periodic.commit('
            MATCH (t1:Twitter)-[r:TEAM|AVATAR|BEEP]->(t2:Twitter)
            WITH r
            LIMIT 10000
            DELETE r
            RETURN count(*)
        ') 
        yield executions
        """
        self.query(query)

    @count_query_logging
    def ingest_references(self, df=None):
        count = 0
        for index, row in df.iterrows():
            twitterId = row['id']
            handle = row['handles']
            handle = handle.replace("@", "")
            create_handle = f"""
            MERGE
                (t:Twitter {{handle: "{handle}"}})
            ON CREATE
                SET 
                    t.uuid = apoc.create.uuid(),
                    t.createdDt = timestamp(),
                    t.lastUpdateDt = timestamp()
            ON MATCH
                SET
                    t.lastUpdateDt = timestamp()
            RETURN
                count(t)
            """
            count += self.query(create_handle)[0].value()
            query = f"""
            MATCH (t:Twitter)
            WHERE id(t) = {twitterId}
            MATCH (tt:Twitter)
            WHERE tt.handle = '{handle}'
            WITH t, tt
            MERGE (t)-[r:IDENTIFIES_WITH]->(tt)
            RETURN count(distinct(t))
            """
            count += self.query(query)[0].value()
            logging.info(f"""logged {count} records...""")

        return count 

    ## collects twitter accounts with websites that have no link between twitter & website
    def get_twitter_websites(self):
        query = """
        MATCH 
            (t:Twitter)
        WHERE 
            t.website is not null 
        AND NOT 
            (t)-[:HAS_WEBSITE]->()
        RETURN distinct
            t.userId as userId,
            t.website as website
            """
        results = self.query(query)
        results_df = pd.DataFrame(results)
        len_df = len(results_df)
        logging.info(f"Okay I collected {len_df} websites.")
        logging.info(results_df.head(5))

        return results_df 

    @count_query_logging
    def create_domains(self, urls):
        count = 0 
        for url in urls: 
            domains_query = f"""
            WITH datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as ingestDate
            LOAD CSV WITH HEADERS FROM {url} as websites
            MERGE (domain:Domain {{domain: websites.domain}})
            ON MATCH 
                SET
                    domain.lastUpdateDt = ingestDate
            ON CREATE
                SET
                    domain.createdDt = ingestDate,
                    domain.uuid = apoc.create.uuid()
            RETURN 
                COUNT(DISTINCT(domain))
            """
            count += self.query(domains_query)[0].value()
        return count 

    @count_query_logging
    def create_websites(self, urls):
        count = 0 
        for url in urls: 
            websites_query = f"""
            WITH datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as ingestDate
            LOAD CSV WITH HEADERS FROM {url} as websites
            MERGE (website:Website {{url: websites.url}})
            ON MATCH 
                SET
                    website.lastUpdateDt = ingestDate
            ON CREATE
                SET
                    website.createdDt = ingestDate,
                    website.uuid = apoc.create.uuid()
            RETURN 
                COUNT(DISTINCT(website))
            """
            count += self.query(websites_query)[0].value()
            
        return count 

    @count_query_logging
    def link_websites_domains(self, urls):
        count = 0 
        for url in urls:
            link_website_domain_query = f"""
            WITH 
                datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as ingestDate
            LOAD CSV WITH HEADERS FROM {url} as websites
            MATCH (website:Website {{url: websites.url}})
            MATCH (domain:Domain {{domain: websites.domain}})
            WITH website, domain, ingestDate
            MERGE (website)-[r:HAS_DOMAIN]->(domain)
            SET r.createdDt = ingestDate
            SET r.lastUpdateDt = ingestDate
            RETURN COUNT(DISTINCT(website))"""

            count += self.query(link_website_domain_query)[0].value()

        return count 