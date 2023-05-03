from tqdm import tqdm
from ...helpers import Cypher
from ...helpers import Queries
from ...helpers import count_query_logging


class FarcasterCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()

    def create_indexes(self):
        query = "CREATE INDEX UniqueFarcasterID IF NOT EXISTS FOR (n:Farcaster) ON (n.id)"
        self.query(query)

    @count_query_logging
    def create_or_merge_farcaster_users(self, urls):
        count = 0
        for url in tqdm(urls):
            profile_query = f"""
                                LOAD CSV WITH HEADERS FROM '{url}' as users
                                MERGE (a:Farcaster:Account {{id: toInteger(users.fid)}})
                                ON MATCH SET
                                    a.fname = users.fname,
                                    a.name = users.name,
                                    a.bio = users.bio,
                                    a.profileImageUrl = users.profileUrl,
                                    a.url = users.url,
                                    a.address = toLower(users.address),
                                    a.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                                ON CREATE SET
                                    a.fname = users.fname,
                                    a.name = users.name,
                                    a.bio = users.bio,
                                    a.address = toLower(users.address),
                                    a.profileImageUrl = users.profileUrl,
                                    a.url = users.url,
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
                            MATCH (a:Account:Farcaster {{id: toInteger(users.fid)}}), (w:Wallet {{address: toLower(users.address)}})
                            MERGE (w)-[r:HAS_ACCOUNT]->(a)
                            RETURN COUNT(r)
                """
            count += self.query(profileQuery)[0].value()

        return count
    
    @count_query_logging
    def link_followers(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as users
                        MATCH (a:Farcaster:Account {{id: toInteger(users.fid)}}), (f:Farcaster:Account {{id: toInteger(users.follower)}})
                        MERGE (f)-[r:FOLLOWS]->(a)
                        RETURN COUNT(r)
                    """
            count += self.query(query)[0].value()
        return count
    