from datetime import datetime
from neo4j import GraphDatabase
import os
import logging
import sys


# TODO: Remove this file !


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
