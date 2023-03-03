from tqdm import tqdm
from ...helpers import Cypher
from ...helpers import Queries
from ...helpers import count_query_logging


class FarcasterCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()

    def create_farcaster_wallets(self, urls):
        count = self.queries.create_wallets(urls)
        return count

    @count_query_logging
    def create_or_merge_farcaster_users(self, urls):
        count = 0
        for url in tqdm(urls):
            profile_query = f"""
                                LOAD CSV WITH HEADERS FROM '{url}' as users
                                MERGE (a:Account:Farcaster {{id: users.id}})
                                ON MATCH SET
                                    a.name = users.fname,
                                    a.address = toLower(users.address),
                                    a.url = users.url,
                                    a.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                                ON CREATE SET
                                    a.name = users.fname,
                                    a.address = toLower(users.address),
                                    a.url = users.url,
                                    a.fId = toInteger(users.fId),
                                    a.uuid = apoc.create.uuid(),
                                    a.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                    a.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                                RETURN
                                    COUNT(DISTINCT(a))
                        """
            count += self.query(profile_query)[0].value()
        return count

    @count_query_logging
    def link_users_wallets(self, urls):
        count = 0
        for url in tqdm(urls):
            profileQuery = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' as users
                            MATCH (a:Account:Farcaster {{id: users.id}}), (w:Wallet {{address: toLower(users.address)}})
                            MERGE (w)-[r:HAS_ACCOUNT]->(a)
                            RETURN COUNT(r)
                """
            count += self.query(profileQuery)[0].value()

        return count
    