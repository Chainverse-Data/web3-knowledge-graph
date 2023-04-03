from .cypher import Cypher


class Constraints(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    def create_constraints(self) -> None:
        pass

    def create_indexes(self) -> None:
        pass

    def contracts(self) -> None:
        query = """CREATE CONSTRAINT UniqueAddress IF NOT EXISTS FOR (d:Contract) REQUIRE d.address IS UNIQUE"""
        self.query(query)

    def twitter(self) -> None:
        query = """CREATE CONSTRAINT UniqueHandle IF NOT EXISTS FOR (d:Twitter) REQUIRE d.handle IS UNIQUE"""
        self.query(query)

    def wallets(self) -> None:
        query = """CREATE CONSTRAINT UniqueAddress IF NOT EXISTS FOR (w:Wallet) REQUIRE w.address IS UNIQUE"""
        self.query(query)

    def tokens(self) -> None:
        query = """CREATE CONSTRAINT UniqueTokenAddress IF NOT EXISTS FOR (d:Token) REQUIRE d.address IS UNIQUE"""
        self.query(query)

    def transactions(self) -> None:
        query = """CREATE CONSTRAINT UniqueTransaction IF NOT EXISTS FOR (d:Transaction) REQUIRE d.txHash IS UNIQUE"""
        self.query(query)

    def aliases(self) -> None:
        query = """CREATE CONSTRAINT UniqueAlias IF NOT EXISTS FOR (d:Alias) REQUIRE d.name IS UNIQUE"""
        self.query(query)

    def ens(self) -> None:
        query = """CREATE CONSTRAINT UniqueENS IF NOT EXISTS FOR (d:Ens) REQUIRE d.name IS UNIQUE"""
        self.query(query)

    def spaces(self) -> None:
        query = """CREATE CONSTRAINT UniqueID IF NOT EXISTS FOR (d:Space) REQUIRE d.snapshotId IS UNIQUE"""
        self.query(query)

    def proposals(self) -> None:
        # it's fine if we have DAOhaus :Proposal and Snapshot :Proposal labels be the same index because the two ids are very different
        query = """CREATE CONSTRAINT UniqueID IF NOT EXISTS FOR (d:Proposal) REQUIRE d.snapshotId IS UNIQUE"""
        self.query(query)

    def gitcoin_grants(self) -> None:
        query = """CREATE CONSTRAINT UniqueId IF NOT EXISTS FOR (grant:GitcoinGrant) REQUIRE grant.id IS UNIQUE"""
        self.query(query)

    def gitcoin_users(self) -> None:
        query = """CREATE CONSTRAINT UniqueHandle IF NOT EXISTS FOR (user:GitcoinUser) REQUIRE user.handle IS UNIQUE"""
        self.query(query)

    def gitcoin_bounties(self) -> None:
        query = """CREATE CONSTRAINT UniqueId IF NOT EXISTS FOR (bounty:GitcoinBounty) REQUIRE bounty.id IS UNIQUE"""
        self.query(query)

    def mirror_articles(self) -> None:
        query = """CREATE CONSTRAINT UniqueArticleID IF NOT EXISTS FOR (a:MirrorArticle) REQUIRE a.originalContentDigest IS UNIQUE"""
        self.query(query)

    def daohaus_dao(self) -> None:
        query = """CREATE CONSTRAINT UniqueDaoID IF NOT EXISTS FOR (a:Dao) REQUIRE a.id IS UNIQUE"""
        self.query(query)

    def daohaus_proposal(self) -> None:
        query = """CREATE CONSTRAINT UniqueProposalID IF NOT EXISTS FOR (a:Proposal) REQUIRE a.id IS UNIQUE"""
        self.query(query)

    def website(self) -> None:
        query = """CREATE CONSTRAINT UniqueWebsite IF NOT EXISTS FOR (a:Website) REQUIRE a.url IS UNIQUE"""
        self.query(query)
