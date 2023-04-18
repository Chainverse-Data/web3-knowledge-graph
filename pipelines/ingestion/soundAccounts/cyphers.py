from tqdm import tqdm
from ...helpers import Cypher, Constraints, Indexes, Queries, count_query_logging


class SoundCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()

    def create_constraints(self):
        constraints = Constraints()

    def create_indexes(self):
        indexes = Indexes()
        indexes.sound()

    @count_query_logging
    def create_or_merge_sound_wallets(self, urls):
        count = self.queries.create_wallets(urls)
        return count

    @count_query_logging
    def create_or_merge_sound_twitter(self, urls):
        count = self.queries.create_or_merge_twitter(urls)
        return count

    @count_query_logging
    def create_or_merge_sound_users(self, urls):
        count = 0
        for url in urls:
            sound_user_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as sound
                        MERGE (d:Sound:Account {{handle: toLower(sound.name)}})
                        SET d.url = sound.url,
                            d.accountCreatedDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            d.name = sound.name,
                            d.uuid = apoc.create.uuid(),
                            d.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                        RETURN count(d)
                        """
            count += self.query(sound_user_query)[0].value()
        return count

    @count_query_logging
    def link_sound_wallets(self, urls):
        count = 0
        for url in urls:
            link_sound_wallet_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS sound
                            MATCH (d:Sound {{handle: toLower(sound.name)}})
                            MATCH (w:Wallet {{address: toLower(sound.address)}})
                            MERGE (d)-[r:HAS_ACCOUNT]->(w)
                            ON CREATE set
                                r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                r.citation = sound.url
                            ON MATCH set r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                            return count(r)
                    """
            count += self.query(link_sound_wallet_query)[0].value()
        return count

    @count_query_logging
    def link_sound_twitter(self, urls):
        count = 0
        for url in urls:
            link_sound_account_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS sound
                            MATCH (d:Sound {{handle: toLower(sound.name)}})
                            MATCH (t:Twitter:Account {{handle: toLower(sound.handle)}})
                            MERGE (d)-[r:HAS_ACCOUNT]->(t)
                            ON CREATE set
                                r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                r.citation = sound.url
                            ON MATCH set r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                            return count(r)
                    """
            count += self.query(link_sound_account_query)[0].value()
        return count
