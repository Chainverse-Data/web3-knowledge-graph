from .cypher import Cypher


class Constraints(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    def create_constraints(self):
        pass

    def create_indexes(self):
        pass
    
    def twitter(self):
        query = """CREATE CONSTRAINT UniqueHandle IF NOT EXISTS FOR (d:Twitter) REQUIRE d.handle IS UNIQUE"""
        self.query(query)

    def wallets(self):
        query = """CREATE CONSTRAINT UniqueAddress IF NOT EXISTS FOR (w:Wallet) REQUIRE w.address IS UNIQUE"""
        self.query(query)

    def tokens(self):
        query = """CREATE CONSTRAINT UniqueTokenAddress IF NOT EXISTS FOR (d:Token) REQUIRE d.address IS UNIQUE"""
        self.query(query)

    def transactions(self):
        query = """CREATE CONSTRAINT UniqueTransaction IF NOT EXISTS FOR (d:Transaction) REQUIRE d.txHash IS UNIQUE"""
        self.query(query)

    def aliases(self):
        query = """CREATE CONSTRAINT UniqueAlias IF NOT EXISTS FOR (d:Alias) REQUIRE d.name IS UNIQUE"""
        self.query(query)

    def ens(self):
        query = """CREATE CONSTRAINT UniqueENS IF NOT EXISTS FOR (d:Ens) REQUIRE d.editionId IS UNIQUE"""
        self.query(query)

    def spaces(self):
        query = """CREATE CONSTRAINT UniqueID IF NOT EXISTS FOR (d:Space) REQUIRE d.snapshotId IS UNIQUE"""
        self.query(query)

    def proposals(self):
        # it's fine if we have DAOhaus :Proposal and Snapshot :Proposal labels be the same index because the two ids are very different
        query = """CREATE CONSTRAINT UniqueID IF NOT EXISTS FOR (d:Proposal) REQUIRE d.snapshotId IS UNIQUE"""
        self.query(query)

    def mirror(self):
        query = """CREATE CONSTRAINT UniqueMirror IF NOT EXISTS FOR (d:Mirror) REQUIRE d.uri IS UNIQUE"""
        self.query(query)

    def gitcoin_grants(self):
        query = """CREATE CONSTRAINT UniqueId IF NOT EXISTS FOR (grant:GitcoinGrant) REQUIRE grant.id IS UNIQUE"""
        self.query(query)

    def gitcoin_users(self):
        query = """CREATE CONSTRAINT UniqueHandle IF NOT EXISTS FOR (user:GitcoinUser) REQUIRE user.handle IS UNIQUE"""
        self.query(query)

    def gitcoin_bounties(self):
        query = """CREATE CONSTRAINT UniqueId IF NOT EXISTS FOR (bounty:GitcoinBounty) REQUIRE bounty.id IS UNIQUE"""
        self.query(query)
