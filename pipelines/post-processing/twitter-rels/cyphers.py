import logging
from datetime import datetime
from datetime import timedelta
import pandas as pd
from ...helpers import Cypher
from ...helpers import count_query_logging, get_query_logging


class TwitterRelsCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    def get_bios(self):
        ingestTime = datetime.now()
        oneMonthAgo = ingestTime - timedelta(days=30)
        query = f"""
        match (twitter:Twitter)
        where not twitter.bio is null
        and twitter.lastProcessingDateBio is null
        return distinct twitter.userId as userId, twitter.bio as bio 
        union 
        match (twitter:Twitter)
        where twitter.bio is null 
        and twitter.lastProcessingDateBio > datetime(epochMillis: {oneMonthAgo.timestamp()})
        with twitter.userId as userId, twitter.bio as bio
        return distinct userId, bio
        """
        results = pd.DataFrame(query)
        return results

    def get_websites(self):
        ingestTime = datetime.now()
        twoMonthsAgo = ingestTime - timedelta(days=60)
        query = f"""
        match (twitter:Twitter)
        where not twitter.website is null
        and twitter.lastProcesingDateWebsite is null
        return distinct twitter.userId as userId, twitter.website as website 
        union 
        match (twitter:Twitter)
        where twitter.website is null 
        and twitter.lastProcessingDateWebsite > datetime(epochMillis: {twoMonthsAgo.timestamp()})
        with twitter.userId as userId, twitter.website as website
        return distinct userId, website
        """
        results = pd.DataFrame(query)
        return results
    
    def delete_lame_rels(self):
        query = """
        call apoc.periodic.commit('
        match (t:Twitter)-[r:TEAM|AVATAR|BEEP]->(t1:Twitter)
        with r
        limit 10000
        delete r
        return count(*)
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
            merge
                (t:Twitter {{handle: "{handle}"}})
            on create
                set 
                    t.uuid = apoc.create.uuid(),
                    t.createdDt = timestamp(),
                    t.lastUpdateDt = timestamp()
            on match
                set
                    t.lastUpdateDt = timestamp()
            return
                count(t)
            """
            count += self.query(create_handle)[0].value()
            query = f"""
            match (t:Twitter)
            where id(t) = {twitterId}
            match (tt:Twitter)
            where tt.handle = '{handle}'
            with t, tt
            merge (t)-[r:IDENTIFIES_WITH]->(tt)
            return count(distinct(t))
            """
            count += self.query(query)[0].value()
            logging.info(f"""logged {count} records...""")

        return count 

    ## collects twitter accounts with websites that have no link between twitter & website
    def get_twitter_websites(self):
        query = """
        match 
            (t:Twitter)
        where 
            t.website is not null 
        and not 
            (t)-[:HAS_WEBSITE]->()
        return distinct
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






        
        


    def run(self):
        #self.cyphers.delete_lame_rels()
       # self.ingest_stuff()
        logging.info("Ingesting websites")
        self.get_websites()

