from datetime import datetime


def create_constraints(conn):
    twitter_query = """CREATE CONSTRAINT UniqueHandle IF NOT EXISTS FOR (d:Twitter) REQUIRE d.handle IS UNIQUE"""
    conn.query(twitter_query)

    wallet_query = """CREATE CONSTRAINT UniqueAddress IF NOT EXISTS FOR (w:Wallet) REQUIRE w.address IS UNIQUE"""
    conn.query(wallet_query)

    token_query = """CREATE CONSTRAINT UniqueTokenAddress IF NOT EXISTS FOR (d:Token) REQUIRE d.address IS UNIQUE"""
    conn.query(token_query)

    transaction_query = (
        """CREATE CONSTRAINT UniqueTransaction IF NOT EXISTS FOR (d:Transaction) REQUIRE d.txHash IS UNIQUE"""
    )
    conn.query(transaction_query)

    alias_query = """CREATE CONSTRAINT UniqueAlias IF NOT EXISTS FOR (d:Alias) REQUIRE d.name IS UNIQUE"""
    conn.query(alias_query)

    ens_query = """CREATE CONSTRAINT UniqueENS IF NOT EXISTS FOR (d:Ens) REQUIRE d.editionId IS UNIQUE"""
    conn.query(ens_query)

    space_query = """CREATE CONSTRAINT UniqueID IF NOT EXISTS FOR (d:Space) REQUIRE d.snapshotId IS UNIQUE"""
    conn.query(space_query)
    # it's fine if we have DAOhaus :Proposal and Snapshot :Proposal labels be the same index because the two ids are very different
    proposal_query = """CREATE CONSTRAINT UniqueID IF NOT EXISTS FOR (d:Proposal) REQUIRE d.snapshotId IS UNIQUE"""
    conn.query(proposal_query)

    mirror_query = """CREATE CONSTRAINT UniqueMirror IF NOT EXISTS FOR (d:Mirror) REQUIRE d.uri IS UNIQUE"""
    conn.query(mirror_query)

    daohaus_query = """CREATE CONSTRAINT UniqueDAOhausID IF NOT EXISTS FOR (d:DaoHaus) REQUIRE d.daohausId IS UNIQUE"""
    conn.query(daohaus_query)


# create all indexes for nodes
def create_indexes(conn):
    proposal_query = """CREATE INDEX UniquePropID IF NOT EXISTS FOR (n:Proposal) ON (n.snapshotId)"""
    conn.query(proposal_query)

    space_query = """CREATE INDEX UniqueSpaceID IF NOT EXISTS FOR (n:Space) ON (n.snapshotId)"""
    conn.query(space_query)

    wallet_query = """CREATE INDEX UniqueAddress IF NOT EXISTS FOR (n:Wallet) ON (n.address)"""
    conn.query(wallet_query)

    ens_query = """CREATE INDEX UniqueENS IF NOT EXISTS FOR (n:Ens) ON (n.editionId)"""
    conn.query(ens_query)

    transaction_query = """CREATE INDEX UniqueTransaction IF NOT EXISTS FOR (n:Transaction) ON (n.txHash)"""
    conn.query(transaction_query)

    alias_query = """CREATE INDEX UniqueAlias IF NOT EXISTS FOR (n:Alias) ON (n.name)"""
    conn.query(alias_query)

    article_query = """CREATE INDEX UniqueArticleID IF NOT EXISTS FOR (n:Mirror) ON (n.uri)"""
    conn.query(article_query)

    twitter_query = """CREATE INDEX UniqueTwitterID IF NOT EXISTS FOR (n:Twitter) ON (n.handle)"""
    conn.query(twitter_query)

    daohaus_query = """CREATE INDEX UniqueDAOhausID IF NOT EXISTS FOR (n:DaoHaus) ON (n.daohausId)"""
    conn.query(daohaus_query)


# multsig labels
def add_multisig_labels(url, conn):

    wallet_node_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                            MATCH (w:Wallet {{address: toLower(wallets.multisig)}})
                            SET w:MultiSig,
                                w.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                w.threshold = toInteger(wallets.threshold)
                            return count(w)
                        """

    x = conn.query(wallet_node_query)
    print("MultiSig Labels Addeds", x)


# wallet nodes
def merge_wallet_nodes(url, conn):

    wallet_node_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                            MERGE (w:Wallet {{address: toLower(wallets.address)}})
                            ON CREATE set w.uuid = apoc.create.uuid(),
                                w.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                w.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                            return count(w)
                        """

    x = conn.query(wallet_node_query)
    print("wallet nodes merged", x)


# token nodes
def merge_token_nodes(url, conn):

    token_node_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS tokens
                        MERGE(t:Token {{address: toLower(tokens.address)}})
                        ON CREATE set t = tokens,
                        t.uuid = apoc.create.uuid()
                        return count(t)
                    """

    x = conn.query(token_node_query)
    print("token nodes merged", x)


# ens nodes
def merge_ens_nodes(url, conn):

    ens_node_query = f"""
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

    x = conn.query(ens_node_query)
    print("ens nodes merged", x)


def merge_ens_relationships(url, conn):

    ens_rel_query = f"""
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

    x = conn.query(ens_rel_query)
    print("ens relationships created", x)


# twitter nodes
def merge_twitter_nodes(url, conn):

    twitter_node_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS twitter
                        MERGE (a:Twitter {{handle: toLower(twitter.handle)}})
                        ON CREATE set a.uuid = apoc.create.uuid(),
                            a.profileUrl = twitter.profileUrl,
                            a.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            a.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            a:Account

                        return count(a)    
                        """

    x = conn.query(twitter_node_query)
    print("twitter nodes merged", x)
