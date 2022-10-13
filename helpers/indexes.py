from .cypher import Cypher

class Indexes(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    def proposals(self):
        query = """CREATE INDEX UniquePropID IF NOT EXISTS FOR (n:Proposal) ON (n.snapshotId)"""
        self.query(query)

    def spaces(self):
        query = """CREATE INDEX UniqueSpaceID IF NOT EXISTS FOR (n:Space) ON (n.snapshotId)"""
        self.query(query)

    def wallets(self):
        query = """CREATE INDEX UniqueAddress IF NOT EXISTS FOR (n:Wallet) ON (n.address)"""
        self.query(query)

    def ens(self):
        query = """CREATE INDEX UniqueENS IF NOT EXISTS FOR (n:Ens) ON (n.editionId)"""
        self.query(query)

    def transactions(self):
        query = """CREATE INDEX UniqueTransaction IF NOT EXISTS FOR (n:Transaction) ON (n.txHash)"""
        self.query(query)

    def aliases(self):
        query = """CREATE INDEX UniqueAlias IF NOT EXISTS FOR (n:Alias) ON (n.name)"""
        self.query(query)

    def articles(self):
        query = """CREATE INDEX UniqueArticleID IF NOT EXISTS FOR (n:Mirror) ON (n.uri)"""
        self.query(query)

    def twitter(self):
        query = """CREATE INDEX UniqueTwitterID IF NOT EXISTS FOR (n:Twitter) ON (n.handle)"""
        self.query(query)

    def gitcoin_grants(self):
        query = """CREATE INDEX UniqueGrantID IF NOT EXISTS FOR (n:EventGitcoinGrant) ON (n.id)"""
        self.query(query)

    def gitcoin_users(self):
        query = """CREATE INDEX UniqueUserHandle IF NOT EXISTS FOR (n:UserGitcoin) ON (n.handle)"""
        self.query(query)

    def gitcoin_bounties(self):
        query = """CREATE INDEX UniqueBountyID IF NOT EXISTS FOR (n:EventGitcoinBounty) ON (n.id)"""
        self.query(query)
