from tqdm import tqdm
from ...helpers import Cypher, Constraints, Indexes, Queries, count_query_logging


class DuneCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()

    def create_constraints(self):
        constraints = Constraints()
        constraints.ens()

    def create_indexes(self):
        indexes = Indexes()
        indexes.ens()

    @count_query_logging
    def create_or_merge_dune_wallets(self, urls):
        count = self.queries.create_wallets(urls)
        return count

    @count_query_logging
    def create_or_merge_dune_twitter(self, urls):
        count = self.queries.create_or_merge_twitter(urls)
        return count
    
    @count_query_logging
    def create_or_merge_dune_users(self, urls):
        count = 0
        for url in urls:
            channel_node_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' as dune
                            MERGE (d:Dune:Account {{handle: toLower(dune.name)}})
                            SET d.url = dune.url,
                                d.follows = toInteger(dune.stars),
                                d.bio = dune.description,
                                d.accountCreatedDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                d.name = dune.name,
                                d.uuid = apoc.create.uuid(),
                                d.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                            RETURN count(d)
                        """
            count += self.query(channel_node_query)[0].value()
        return count
    
    @count_query_logging
    def create_or_merge_dune_teams(self, urls):
        count = 0
        for url in urls:
            channel_node_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' as dune
                            MERGE (d:Dune:Account:Entity {{handle: toLower(dune.name)}})
                            SET d.url = dune.url,
                                d.follows = toInteger(dune.stars),
                                d.bio = dune.description,
                                d.accountCreatedDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                d.name = dune.name,
                                d.uuid = apoc.create.uuid(),
                                d.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                            RETURN count(d)
                        """
            count += self.query(channel_node_query)[0].value()
        return count
    
    @count_query_logging
    def create_or_merge_dune_telegram(self, urls):
        count = 0
        for url in urls:
            telegram_node_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS telegram
                            MERGE (t:Telegram {{handle: toLower(telegram.handle)}})
                            ON CREATE set t.uuid = apoc.create.uuid(),
                                t.profileUrl = telegram.url,
                                t.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                t.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                t:Account
                            ON MATCH set t.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                t:Account
                            return count(t)    
                    """
            count += self.query(telegram_node_query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_dune_discord(self, urls):
        count = 0
        for url in urls:
            discord_node_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS discord
                            MERGE (t:Discord {{handle: toLower(discord.handle)}})
                            ON CREATE set t.uuid = apoc.create.uuid(),
                                t.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                t.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                t:Account
                            ON MATCH set t.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                t:Account
                            return count(t)    
                    """
            count += self.query(discord_node_query)[0].value()
        return count
    
    @count_query_logging
    def link_dune_wallets(self, urls):
        count = 0
        for url in urls:
            merge_dune_wallet_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS dune
                            MATCH (d:Dune {{handle: toLower(dune.name)}})
                            MATCH (w:Wallet {{address: toLower(dune.address)}})
                            MERGE (d)-[r:HAS_ACCOUNT]->(w)
                            ON CREATE set
                                r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                r.citation = dune.url
                            ON MATCH set r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                            return count(r)
                    """
            count += self.query(merge_dune_wallet_query)[0].value()
        return count

    @count_query_logging
    def link_dune_accounts(self, urls):
        count = 0
        for url in urls:
            merge_dune_accounts_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS dune
                            MATCH (d:Dune {{handle: toLower(dune.name)}})
                            MATCH (t:Account {{handle: toLower(dune.handle)}})
                            MERGE (d)-[r:HAS_ACCOUNT]->(t)
                            ON CREATE set
                                r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                r.citation = dune.url
                            ON MATCH set r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                            return count(r)
                    """
            count += self.query(merge_dune_accounts_query)[0].value()
        return count

    @count_query_logging
    def link_dune_teams(self, urls):
        count = 0
        for url in urls:
            merge_dune_teams_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS dune
                            MATCH (d:Dune:Account {{handle: toLower(dune.name)}})
                            MATCH (t:Dune:Account:Entity {{handle: toLower(dune.team)}})
                            MERGE (d)-[r:BIO_MENTIONED]->(t)
                            ON CREATE set
                                r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                r.citation = dune.url
                            ON MATCH set r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                            return count(r)
                    """
            count += self.query(merge_dune_teams_query)[0].value()
        return count


