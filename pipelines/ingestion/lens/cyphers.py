import pandas as pd
import logging
from ...helpers import Cypher
from ...helpers import Constraints, Indexes, Queries
from ...helpers import count_query_logging
from tqdm import tqdm

class LensCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()

    def create_indexes(self):
        query = "CREATE INDEX UniqueLensID IF NOT EXISTS FOR (n:Lens) ON (n.name)"
        self.query(query)

    @count_query_logging
    def create_lens_wallets(self, urls):
        count = self.queries.create_wallets(urls)
        return count

    @count_query_logging
    def create_profiles(self, urls):
        count = 0
        for url in tqdm(urls):
            profile_query = f"""
                                LOAD CSV WITH HEADERS FROM '{url}' as profiles
                                MERGE (a:Alias:Lens {{name: profiles.handle}})
                                ON MATCH SET
                                    a.owner = toLower(profiles.owner),
                                    a.creator = toLower(profiles.creator),
                                    a.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                                ON CREATE SET
                                    a.creator = toLower(profiles.creator),
                                    a.owner = toLower(profiles.owner),
                                    a.profileId = toInteger(profiles.profileId),
                                    a.uuid = apoc.create.uuid(),
                                    a.createdOn = datetime({{epochSeconds: toInteger(profiles.createdOn)}}),
                                    a.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                    a.createdDt = datetime(apoc.date.toISO8601(toInteger(profiles.createdOn), 'ms'))
                                RETURN
                                    COUNT(DISTINCT(a))
                        """
            count += self.query(profile_query)[0].value()
        return count

    @count_query_logging
    def link_profiles_wallets(self, urls):
        count = 0
        for url in tqdm(urls):
            profileQuery = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' as profiles
                            MATCH (a:Alias:Lens {{name: profiles.handle}}), (w:Wallet {{address: toLower(profiles.owner)}})
                            MERGE (w)-[r:HAS_ALIAS]->(a)
                            RETURN COUNT(r)
                """
            count += self.query(profileQuery)[0].value()

        return count
    