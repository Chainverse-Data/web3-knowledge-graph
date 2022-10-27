from .cypher import Cypher

# This file is for universal queries only, any queries that generate new nodes or edges must be in its own cyphers.py file in the service folder


class Queries(Cypher):
    """This class holds queries for general nodes such as Wallet or Twitter"""

    def __init__(self, database=None):
        super().__init__(database)

    def create_constraints(self):
        pass

    def create_indexes(self):
        pass

    def create_wallets(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS admin_wallets
                    MERGE(wallet:Wallet {{address: toLower(admin_wallets.address)}})
                    ON CREATE set wallet.uuid = apoc.create.uuid(),
                        wallet.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        wallet.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        wallet.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set wallet.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        wallet.ingestedBy = "{self.UPDATED_ID}"
                    return count(wallet)
            """
            count += self.query(query)[0].value()
        return count

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

    def create_or_merge_twitter(self, urls):
        count = 0
        for url in urls:

            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS twitter
                    MERGE (t:Twitter {{handle: toLower(twitter.handle)}})
                    ON CREATE set t.uuid = apoc.create.uuid(),
                        t.profileUrl = twitter.profileUrl,
                        t.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        t.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        t.ingestedBy = "{self.CREATED_ID}",
                        t:Account
                    ON MATCH set t.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        t.ingestedBy = "{self.UPDATED_ID}"
                    return count(t)    
            """
            count += self.query(query)[0].value()
        return count

    def create_or_merge_alias(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS alias
                    MERGE (a:Alias {{name: toLower(alias.name)}})
                    ON CREATE set a.uuid = apoc.create.uuid(),
                        a.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        a.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(a)
                    """

            count += self.query(query)[0].value()
        return count

    def create_or_merge_ens_nft(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS ens
                    MERGE (e:Ens:Nft {{editionId: ens.tokenId}})
                    ON CREATE set e.uuid = apoc.create.uuid(),
                        e.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        e.contractAddress = ens.contractAddress
                    return count(e)
                    """

            count += self.query(query)[0].value()
        return count

    def create_or_merge_transaction(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS tx
                    MERGE (t:Transaction {{txHash: toLower(tx.txHash)}})
                    ON CREATE set t.uuid = apoc.create.uuid(),
                        t.date = datetime(apoc.date.toISO8601(toInteger(tx.date), 's')),
                        t:Event
                    return count(t)
                    """

            count += self.query(query)[0].value()
        return count

    def link_wallet_alias(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS alias
                    MATCH (a:Alias {{name: toLower(alias.name)}}), 
                        (w:Wallet {{address: toLower(alias.address)}})
                    MERGE (w)-[r:HAS_ALIAS]->(a)
                    return count(r)
                    """

            count += self.query(query)[0].value()
        return count

    def link_wallet_transaction_ens(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS tx
                    MATCH (t:Transaction {{txHash: toLower(tx.txHash)}}), 
                        (e:Ens {{editionId: tx.tokenId}})
                    MERGE (w)-[r:RECEIVED]->(t)
                    return count(r)
                    """

            count += self.query(query)[0].value()
        return count

    def link_ens_transaction(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS tx
                    MATCH (t:Transaction {{txHash: toLower(tx.txHash)}}), 
                        (e:Ens {{editionId: tx.tokenId}})
                    MERGE (e)-[r:TRANSFERRED]->(t)
                    return count(t)
                    """

            count += self.query(query)[0].value()

    def link_ens_alias(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS ens
                    MATCH (e:Ens {{editionId: ens.tokenId}}), 
                        (a:Alias {{name: toLower(ens.name)}})
                    MERGE (e)-[r:HAS_NAME]->(a)
                    return count(r)
                    """

            count += self.query(query)[0].value()
        return count
