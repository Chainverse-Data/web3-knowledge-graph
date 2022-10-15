from .cypher import Cypher
from .decorators import count_query_logging

# This file is for universal queries only, any queries that generate new nodes or edges must be in its own cyphers.py file in the service folder


class Queries(Cypher):
    """This class holds queries for general nodes such as Wallet or Twitter"""

    def __init__(self, database=None):
        super().__init__(database)

    def create_constraints(self):
        pass

    def create_indexes(self):
        pass

    @count_query_logging
    def create_wallets(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS admin_wallets
                    MERGE(wallet:Wallet {{address: admin_wallets.address}})
                    ON CREATE set wallet.uuid = apoc.create.uuid(),
                        wallet.address = admin_wallets.address,
                        wallet.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        wallet.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set wallet.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        wallet.address = admin_wallets.address
                    return count(wallet)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_tokens(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS tokens
                    MERGE(t:Token {{address: toLower(tokens.address)}})
                    ON CREATE set t = tokens,
                    t.uuid = apoc.create.uuid()
                    return count(t)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_twitter(self, urls):
        count = 0
        for url in urls:

            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS twitter
                    MERGE (a:Twitter {{handle: toLower(twitter.handle)}})
                    ON CREATE set a.uuid = apoc.create.uuid(),
                        a.profileUrl = twitter.profileUrl,
                        a.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        a.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        a:Account
                    ON MATCH set a.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        a:Account
                    return count(a)    
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_ens(self, urls):
        count = 0
        for url in urls:

            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS ens
                    MERGE (a:Alias {{name: toLower(ens.name)}})
                    ON CREATE set a.uuid = apoc.create.uuid(),
                        a.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        a.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    MERGE (w:Wallet {{address: toLower(ens.owner)}})
                    ON CREATE set w.uuid = apoc.create.uuid(),
                            w.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            w.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    MERGE (e:Ens:Nft {{editionId: ens.tokenId}})
                    ON CREATE set e.uuid = apoc.create.uuid(),
                        e.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        e.contractAddress = ens.contractAddress
                    MERGE (t:Transaction {{txHash: toLower(ens.txHash)}})
                    ON CREATE set t.uuid = apoc.create.uuid(),
                        t.date = datetime(apoc.date.toISO8601(toInteger(ens.date), 's')),
                        t.type = 'registrant',
                        t:Event
                    return count(e)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_ens(self, urls):
        count = 0
        for url in urls:

            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' as ens
                    MATCH (w:Wallet {{address: toLower(ens.owner)}}), 
                        (e:Ens {{editionId: ens.tokenId}}), 
                        (a:Alias {{name: toLower(ens.name)}}),
                        (t:Transaction {{txHash: toLower(ens.txHash)}})
                    MERGE (w)-[n:HAS_ALIAS]->(a)
                    MERGE (w)-[:RECEIVED]->(t)
                    MERGE (e)-[:TRANSFERRED]->(t)
                    MERGE (e)-[:HAS_NAME]->(a)
                    return count(n)
            """
            count += self.query(query)[0].value()
        return count