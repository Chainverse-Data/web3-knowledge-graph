

from ...helpers import get_query_logging, count_query_logging
from ...helpers import Cypher
import os

DEBUG = os.environ.get("DEBUG", False)

class ENSLooupCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)
    
    @get_query_logging
    def get_ens_names(self):
        query = f"""
            MATCH (ens:Alias:Ens)
            WHERE ens.textRecordScrappedDt IS NULL
            RETURN ens.name as name
        """
        if DEBUG:
            query += "LIMIT 10"
        results = self.query(query)
        return results
    
    # @count_query_logging
    # def clean_alias_links(self, urls):
    #     count = 0
    #     for url in urls:
    #         query = f"""
    #             LOAD CSV WITH HEADERS FROM '{url}' AS aliases
    #             MATCH (Alias:Ens {{name: toLower(aliases.name)}})-[link:HAS_ALIAS]-(Wallet)
    #             SET link.old_link = true
    #             RETURN count(link)
    #         """
    #         count += self.query(query)[0].value()
    #     return count
    
    # @count_query_logging
    # def link_or_merge_alias_links(self, urls):
    #     count = 0
    #     for url in urls:
    #         query = f"""
    #             LOAD CSV WITH HEADERS FROM '{url}' AS aliases
    #             MATCH (alias:Alias:Ens {{name: toLower(aliases.name)}})
    #             MATCH (wallet:Wallet {{address: toLower(aliases.address)}})
    #             MERGE (wallet)-[link:HAS_ALIAS]->(alias)
    #             SET link.old_link = null
    #             RETURN count(link)
    #         """
    #         count += self.query(query)[0].value()
    #     return count
    
    @count_query_logging
    def update_ens(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS aliases
                MATCH (alias:Alias:Ens {{name: toLower(aliases.name)}})
                SET alias.avatar = aliases.avatar,
                    alias.description = aliases.description,
                    alias.display = aliases.display,
                    alias.email = aliases.email,
                    alias.keywords = aliases.keywords,
                    alias.mail = aliases.mail,
                    alias.notice = aliases.notice,
                    alias.location = aliases.location,
                    alias.phone = aliases.phone,
                    alias.url = aliases.url,
                    alias.github = aliases.github,
                    alias.peepeth = aliases.peepeth,
                    alias.linkedin = aliases.linkedin,
                    alias.twitter = aliases.twitter,
                    alias.keybase = aliases.keybase,
                    alias.telegram = aliases.telegram,
                    alias.textRecordScrappedDt = datetime(),
                    alias.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    alias.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    alias.ingestedBy = "{self.UPDATED_ID}"
                RETURN count(alias)
            """
            count += self.query(query)[0].value()
        return count
    
    @count_query_logging
    def create_or_merge_records(self, urls, label, key):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS records
                MERGE (account:{label}:Account {{{key}: toLower(records.{key})}})
                ON CREATE SET   account.uuid = apoc.create.uuid(),
                                account.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                account.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                ON MATCH SET    account.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                RETURN count(account)
            """
            count += self.query(query)[0].value()
        return count
    

    @count_query_logging
    def link_or_merge_records(self, urls, label, key):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS records
                MATCH (account:{label}:Account {{{key}: toLower(records.{key})}})
                MATCH (alias:Alias:Ens {{name: toLower(records.name)}})
                MERGE (alias)-[link:HAS_ACCOUNT]->(account)
                ON CREATE SET   link.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                link.citation = "Set in ENS text records",
                                link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                ON MATCH SET    link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                link.citation = "Set in ENS text records"
                RETURN count(link)
            """
            count += self.query(query)[0].value()
        return count